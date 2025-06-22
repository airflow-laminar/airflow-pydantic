import ast
import os
from logging import getLogger
from pathlib import Path
from shutil import which
from subprocess import call
from tempfile import NamedTemporaryFile
from typing import Dict, List

from .task import render_base_task_args
from .utils import _get_parts_from_value

__all__ = ("DagRenderMixin",)

_log = getLogger(__name__)


def _task_id_to_better_name(task_id):
    return task_id.replace("-", "_").replace(" ", "_").replace(".", " ")


class DagRenderMixin:
    def render(self, format: bool = True, debug_filename: str = "") -> str:
        """
        Render the DAG to a string representation, suitable for use in a .py file.
        """
        if not self.dag_id:
            raise ValueError("DAG must have a dag_id")
        imports: List[str] = []
        globals_: List[str] = []
        tasks: Dict[str, str] = {}
        task_dependencies: Dict[str, List[str]] = {}

        new_dag = ast.Module(body=[], type_ignores=[])
        # First, Prepare DAG kwargs
        # dag_kwargs = [ast.keyword("dag_id", ast.Constant(value=self.dag_id))]
        dag_args = self.model_dump(exclude_unset=True, exclude=["type_", "tasks", "default_args"])

        for k, v in dag_args.items():
            new_imports, value = _get_parts_from_value(k, v)
            if new_imports:
                imports.extend(new_imports)
            if isinstance(value, ast.AST):
                # If the value is already an AST node, we can use it directly
                dag_args[k] = value
            else:
                # Otherwise, we need to convert it to an AST node
                dag_args[k] = ast.Constant(value=value)

        if self.default_args:
            dag_default_args_imports, dag_default_args_globals, dag_default_args_values = render_base_task_args(self.default_args, raw=True)
            imports.extend(dag_default_args_imports)
            globals_.extend(dag_default_args_globals)
            dag_args["default_args"] = dag_default_args_values

        # reformat to keywords instance
        dag_args = [ast.keyword(arg=k, value=v) for k, v in dag_args.items()]

        for task_id, task in self.tasks.items():
            task_imports, task_globals, task_code = task.render(raw=True, dag_from_context=True)
            imports.extend(task_imports)
            globals_.extend(task_globals)

            # Ensure task_id is a valid Python identifier
            task_id = _task_id_to_better_name(task_id)

            if not task_id.isidentifier():
                raise ValueError(f"Task ID '{task_id}' is not a valid Python identifier")

            # Add task code to dict
            tasks[task_id] = task_code

            # Grab dependencies and add them
            if task.dependencies:
                to_set = []
                for dependency in task.dependencies:
                    if isinstance(dependency, tuple):
                        # Normalize first element of tuple to task_id
                        dependency = (_task_id_to_better_name(dependency[0]), dependency[1])
                    to_set.append(dependency)
                task_dependencies[task_id] = to_set

        # Imports
        imports.append(ast.ImportFrom(module="airflow.models", names=[ast.alias(name="DAG")], level=0))

        # remove duplicates
        seen = set()
        unique_imports = []

        for _ in imports:
            unparsed = ast.unparse(_)
            if unparsed not in seen:
                seen.add(unparsed)
                unique_imports.append(_)

        # sort
        imports = [x for _, x in sorted(zip([ast.unparse(_) for _ in unique_imports], unique_imports), key=lambda x: x[0])]
        new_dag.body.extend(imports)

        # Globals
        if globals_:
            globals_ = list(set(globals_))  # Remove duplicates
            new_dag.body.extend(globals_)

        # DAG definition
        dag_block = ast.With(
            items=[
                ast.withitem(
                    context_expr=ast.Call(func=ast.Name(id="DAG", ctx=ast.Load()), args=[], keywords=dag_args),
                    optional_vars=ast.Name(id="dag", ctx=ast.Store(), col_offset=0),
                )
            ],
            body=[],
        )

        # Tasks
        # dag_block.body.append(ast.Assign(targets=[ast.Name(id="dag", ctx=ast.Store())], value=dag_block.context_expr))
        for task_id, task in tasks.items():
            dag_block.body.append(
                ast.Assign(
                    targets=[ast.Name(id=task_id, ctx=ast.Store())],
                    value=task,
                )
            )
        if not tasks:
            _log.warning("No tasks found in the DAG. Ensure that tasks are defined correctly.")
            # Add an ellipsis to indicate no tasks
            dag_block.body.append(ast.Expr(value=ast.Constant(value=...)))

        # Handle task dependencies
        for task_id, dependencies in task_dependencies.items():
            for dependency in dependencies:
                # Extract attribute accessor if it exists
                if isinstance(dependency, tuple):
                    dependency, accessor = dependency
                else:
                    accessor = None

                # NOTE: this should have already been validated in the DAG,
                # but do again here for safety
                if dependency not in tasks:
                    raise ValueError(f"Task Dependency for {task_id} not found: {dependency}")

                if accessor:
                    # Access attribute `accessor` of the dependency task
                    dependency_ast = ast.Attribute(
                        value=ast.Name(id=dependency, ctx=ast.Load()),
                        attr=accessor,
                        ctx=ast.Load(),
                    )
                else:
                    dependency_ast = ast.Name(id=dependency)
                dag_block.body.append(
                    ast.Expr(
                        value=ast.BinOp(
                            left=dependency_ast,
                            op=ast.RShift(),
                            right=ast.Name(id=task_id),
                        )
                    )
                )

        # Append dag with statement to body
        new_dag.body.append(dag_block)

        # Fix missing locations
        ast.fix_missing_locations(new_dag)

        # Add comment about generation, assemble final dag via unparse
        ret = "# Generated by airflow-config\n" + ast.unparse(new_dag) + "\n"

        if format:
            _log.info("Formatting the generated code...")
            # Format the code using black
            with NamedTemporaryFile("w+", suffix=".py", delete=False) as temp_file:
                _log.debug("Writing formatted code to temporary file: %s", temp_file.name)
                temp_file.write(ret)
                temp_filename = temp_file.name

            if which("ruff") is not None:
                _log.debug("Using 'ruff' to format the code.")
                # Call ruff to format the file
                call(["ruff", "format", temp_filename])
            elif which("black") is not None:
                _log.debug("Using 'black' to format the code.")
                # Call black to format the file
                call(["black", temp_filename])
            else:
                _log.info("Neither 'ruff' nor 'black' is installed. Please install one of them to format the code.")

            # Read the formatted file
            with open(temp_filename, "r") as f:
                ret = f.read()

            # Clean up the temporary file
            _log.debug("Removing temporary file: %s", temp_filename)
            os.remove(temp_filename)

        # Debug
        if debug_filename:
            _log.debug("Writing debug output to: %s", debug_filename)
            out = Path(debug_filename)
            out.parent.mkdir(parents=True, exist_ok=True)
            if not out.suffix:
                out = out.with_suffix(".py")
            _log.debug("Debug output file path: %s", out)
            out.write_text(ret)
        return ret
