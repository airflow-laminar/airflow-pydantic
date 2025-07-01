from pydantic import BaseModel as PydanticBaseModel, model_validator

__all__ = ("BaseModel",)


class BaseModel(PydanticBaseModel, validate_assignment=True):
    ...

    @model_validator(mode="before")
    @classmethod
    def _apply_template(cls, values):
        if "template" in values:
            template = values.pop("template")
            # Do field-by-field for larger types
            # NOTE: don't use model_dump here as some basemodel fields might be excluded
            for key, value in template.__class__.model_fields.items():
                if key not in template.model_fields_set:
                    # see note above
                    continue
                # Get real value from template
                value = getattr(template, key)
                if key not in values:
                    values[key] = value
                elif isinstance(value, dict):
                    # If the field is a BaseModel, we need to update it
                    # with the new values from the template
                    for subkey, subvalue in value.items():
                        if subkey not in values[key]:
                            values[key][subkey] = subvalue
        return values
