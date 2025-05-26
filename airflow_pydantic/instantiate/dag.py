__all__ = ("DagInstantiateMixin",)


class DagInstantiateMixin:
    def instantiate(self, **kwargs):
        if not self.dag_id:
            raise ValueError("dag_id must be set to instantiate a DAG")
        # args = {**self.model_dump(exclude_none=True, exclude=["type_"]) ** kwargs}
        raise NotImplementedError("DAG instantiation is not implemented in this mixin")
        # return self.operator(dag=dag, **args)
