"""Tests for utility modules."""

from datetime import datetime, timedelta
from typing import Dict, List

import pytest
from pydantic import BaseModel

from airflow_pydantic import Pool, Variable
from airflow_pydantic.utils import BashCommands, in_bash, in_conda, in_virtualenv
from airflow_pydantic.utils.names import _better_task_id, _task_id_to_python_name
from airflow_pydantic.utils.param import ParamType
from airflow_pydantic.utils.path import link


class TestBashCommands:
    """Tests for BashCommands model."""

    def test_bash_commands_basic(self):
        """Test basic BashCommands creation."""
        cmds = BashCommands(commands=["echo 'hello'"])
        result = cmds.model_dump()
        assert "bash" in result
        assert "echo 'hello'" in result

    def test_bash_commands_multiple(self):
        """Test BashCommands with multiple commands."""
        cmds = BashCommands(
            commands=["echo 'first'", "echo 'second'"],
        )
        result = cmds.model_dump()
        assert "echo 'first'" in result
        assert "echo 'second'" in result

    def test_bash_commands_with_cwd(self):
        """Test BashCommands with working directory."""
        cmds = BashCommands(
            commands=["ls -la"],
            cwd="/tmp",
        )
        result = cmds.model_dump()
        assert "cd" in result
        assert "/tmp" in result

    def test_bash_commands_with_env(self):
        """Test BashCommands with environment variables."""
        cmds = BashCommands(
            commands=["echo $VAR"],
            env={"VAR": "value"},
        )
        result = cmds.model_dump()
        assert "VAR" in result
        assert "value" in result

    def test_bash_commands_no_login(self):
        """Test BashCommands without login shell."""
        cmds = BashCommands(
            commands=["echo 'test'"],
            login=False,
        )
        result = cmds.model_dump()
        assert "-lc" not in result
        assert "-c" in result


class TestInBash:
    """Tests for in_bash function."""

    def test_in_bash_basic(self):
        """Test basic in_bash."""
        result = in_bash("echo 'hello'")
        assert result.startswith("bash")
        assert "echo 'hello'" in result

    def test_in_bash_no_login(self):
        """Test in_bash without login shell."""
        result = in_bash("echo 'hello'", login=False)
        assert "-lc" not in result
        assert "-c" in result

    def test_in_bash_with_escape(self):
        """Test in_bash with escaping."""
        result = in_bash("echo 'hello'", escape=True, quote=False)
        assert "bash" in result

    def test_in_bash_with_cwd(self):
        """Test in_bash with working directory."""
        result = in_bash("ls", cwd="/tmp")
        assert "cd" in result
        assert "/tmp" in result

    def test_in_bash_with_env(self):
        """Test in_bash with environment variables."""
        result = in_bash("echo $VAR", env={"VAR": "test"})
        assert "export VAR" in result


class TestInConda:
    """Tests for in_conda function."""

    def test_in_conda_basic(self):
        """Test basic in_conda."""
        result = in_conda("myenv", "python script.py")
        assert "micromamba activate myenv" in result
        assert "python script.py" in result

    def test_in_conda_custom_tool(self):
        """Test in_conda with custom tool."""
        result = in_conda("myenv", "python script.py", tool="conda")
        assert "conda activate myenv" in result


class TestInVirtualenv:
    """Tests for in_virtualenv function."""

    def test_in_virtualenv_basic(self):
        """Test basic in_virtualenv."""
        result = in_virtualenv("/opt/venv", "python script.py")
        assert "source /opt/venv/bin/activate" in result
        assert "python script.py" in result


class TestNames:
    """Tests for name utility functions."""

    def test_task_id_to_python_name_basic(self):
        """Test basic task ID to Python name conversion."""
        assert _task_id_to_python_name("my-task") == "my_task"
        assert _task_id_to_python_name("my_task") == "my_task"
        assert _task_id_to_python_name("my task") == "my_task"
        assert _task_id_to_python_name("my.task") == "my_task"

    def test_task_id_to_python_name_invalid(self):
        """Test invalid Python identifier."""
        with pytest.raises(ValueError):
            _task_id_to_python_name("123-invalid")

    def test_better_task_id(self):
        """Test better task ID generation."""
        assert _better_task_id("my_task") == "my-task"
        assert _better_task_id("my task") == "my-task"
        assert _better_task_id("my.task") == "my-task"


class TestPath:
    """Tests for path utility functions."""

    def test_link_with_unlink(self):
        """Test link with unlink option."""
        result = link("/source", "/target", unlink=True)
        assert result == "ln -sfn /source /target"

    def test_link_without_unlink(self):
        """Test link without unlink option."""
        result = link("/source", "/target", unlink=False)
        assert result == "ln -s /source /target"


class TestPool:
    """Tests for Pool model."""

    def test_pool_from_dict(self):
        """Test Pool creation from dict."""
        pool = Pool(pool="test-pool", slots=5, description="A test pool")
        assert pool.pool == "test-pool"
        assert pool.slots == 5
        assert pool.description == "A test pool"

    def test_pool_roundtrip(self):
        """Test Pool serialization roundtrip."""
        pool = Pool(pool="test-pool", slots=10)
        dumped = pool.model_dump(exclude_unset=True)
        restored = Pool.model_validate(dumped)
        assert pool.pool == restored.pool
        assert pool.slots == restored.slots


class TestVariable:
    """Tests for Variable model."""

    def test_variable_from_dict(self):
        """Test Variable creation from dict."""
        var = Variable(key="MY_VAR", description="A test variable")
        assert var.key == "MY_VAR"
        assert var.description == "A test variable"

    def test_variable_deserialize_json(self):
        """Test Variable with deserialize_json option."""
        var = Variable(key="CONFIG", deserialize_json=True)
        assert var.deserialize_json is True

    def test_variable_roundtrip(self):
        """Test Variable serialization roundtrip."""
        var = Variable(key="MY_VAR", description="test", is_encrypted=True)
        dumped = var.model_dump(exclude_unset=True)
        restored = Variable.model_validate(dumped)
        assert var.key == restored.key
        assert var.description == restored.description


class TestParamType:
    """Tests for ParamType class."""

    def test_resolve_type_string(self):
        """Test type resolution for string."""
        assert ParamType._resolve_type(str) == "string"

    def test_resolve_type_integer(self):
        """Test type resolution for integer."""
        assert ParamType._resolve_type(int) == "integer"

    def test_resolve_type_number(self):
        """Test type resolution for float."""
        assert ParamType._resolve_type(float) == "number"

    def test_resolve_type_boolean(self):
        """Test type resolution for boolean."""
        assert ParamType._resolve_type(bool) == "boolean"

    def test_resolve_type_list(self):
        """Test type resolution for list."""
        assert ParamType._resolve_type(list) == "array"
        assert ParamType._resolve_type([1, 2, 3]) == "array"

    def test_resolve_type_dict(self):
        """Test type resolution for dict instance."""
        # Note: dict type returns None, but dict instances return "object"
        assert ParamType._resolve_type({"a": 1}) == "object"

    def test_resolve_type_none(self):
        """Test type resolution for None value."""
        # Note: None value returns None (not "null") because it's not a type
        # The "null" return happens only for type(None) which is NoneType
        assert ParamType._resolve_type(None) is None

    def test_resolve_type_datetime(self):
        """Test type resolution for datetime."""
        assert ParamType._resolve_type(datetime) == "number"

    def test_resolve_type_timedelta(self):
        """Test type resolution for timedelta."""
        assert ParamType._resolve_type(timedelta) == "number"

    def test_resolve_type_basemodel(self):
        """Test type resolution for BaseModel."""

        class TestModel(BaseModel):
            pass

        assert ParamType._resolve_type(TestModel) == "object"

    def test_resolve_type_generic_dict(self):
        """Test type resolution for generic Dict."""
        assert ParamType._resolve_type(Dict[str, int]) == "object"

    def test_resolve_type_generic_list(self):
        """Test type resolution for generic List."""
        assert ParamType._resolve_type(List[int]) == "array"

    def test_resolve_type_unknown(self):
        """Test type resolution for unknown type returns None."""
        # A simple object that's not a recognized type
        assert ParamType._resolve_type(object) is None
