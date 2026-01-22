"""Tests for the core BaseModel functionality."""

import os

import pytest

from airflow_pydantic.core.base import BaseModel


class TestBaseModel:
    """Tests for BaseModel class."""

    def test_base_model_creation(self):
        """Test basic BaseModel creation."""

        class TestModel(BaseModel):
            name: str
            value: int = 10

        model = TestModel(name="test")
        assert model.name == "test"
        assert model.value == 10

    def test_base_model_validate_assignment(self):
        """Test that validate_assignment works."""

        class TestModel(BaseModel):
            name: str
            value: int = 10

        model = TestModel(name="test")
        model.name = "updated"
        assert model.name == "updated"

    @pytest.mark.skipif(os.environ.get("AIRFLOW_PYDANTIC_ENABLE_CCFLOW"), reason="CCFlow integration enabled")
    def test_model_dump_excludes_type_(self):
        """Test that model_dump excludes type_ field."""

        class TestModel(BaseModel):
            name: str
            type_: str = "test_type"

        model = TestModel(name="test")
        dumped = model.model_dump()
        assert "type_" not in dumped
        assert "name" in dumped

    @pytest.mark.skipif(os.environ.get("AIRFLOW_PYDANTIC_ENABLE_CCFLOW"), reason="CCFlow integration enabled")
    def test_model_dump_json_excludes_type_(self):
        """Test that model_dump_json excludes type_ field."""

        class TestModel(BaseModel):
            name: str
            type_: str = "test_type"

        model = TestModel(name="test")
        json_str = model.model_dump_json()
        assert "type_" not in json_str
        assert "name" in json_str

    def test_model_dump_with_exclude(self):
        """Test model_dump with additional exclude."""

        class TestModel(BaseModel):
            name: str
            value: int = 10
            secret: str = "hidden"

        model = TestModel(name="test")
        dumped = model.model_dump(exclude={"secret"})
        assert "name" in dumped
        assert "value" in dumped
        assert "secret" not in dumped

    def test_template_application(self):
        """Test that template is applied correctly."""

        class TestModel(BaseModel):
            name: str
            value: int = 10
            extra: str = "default"

        template = TestModel(name="template", value=20, extra="template_extra")
        model = TestModel(name="new", template=template)

        # name is provided, so it should use the provided value
        assert model.name == "new"
        # value should come from template since not provided
        assert model.value == 20
        # extra should come from template since not provided
        assert model.extra == "template_extra"

    def test_template_override(self):
        """Test that provided values override template values."""

        class TestModel(BaseModel):
            name: str
            value: int = 10
            extra: str = "default"

        template = TestModel(name="template", value=20, extra="template_extra")
        model = TestModel(name="new", value=30, template=template)

        assert model.name == "new"
        assert model.value == 30
        # extra should come from template
        assert model.extra == "template_extra"

    def test_template_with_dict_merge(self):
        """Test template with dict field merging."""

        class TestModel(BaseModel):
            name: str
            config: dict = {}

        template = TestModel(name="template", config={"a": 1, "b": 2})
        model = TestModel(name="new", config={"c": 3}, template=template)

        # config should merge - template values that aren't in new config should be added
        assert model.config["c"] == 3
        assert model.config.get("a") == 1
        assert model.config.get("b") == 2

    def test_template_non_dict_input(self):
        """Test that non-dict inputs are passed through."""

        class TestModel(BaseModel):
            name: str

        # Non-dict input should pass through validation
        with pytest.raises(Exception):
            TestModel("not a dict")

    def test_model_roundtrip(self):
        """Test model serialization roundtrip."""

        class TestModel(BaseModel):
            name: str
            value: int = 10

        original = TestModel(name="test", value=42)
        dumped = original.model_dump(exclude_unset=True)
        restored = TestModel.model_validate(dumped)

        assert original.name == restored.name
        assert original.value == restored.value

    def test_model_json_roundtrip(self):
        """Test model JSON serialization roundtrip."""

        class TestModel(BaseModel):
            name: str
            value: int = 10

        original = TestModel(name="test", value=42)
        json_str = original.model_dump_json(exclude_unset=True)
        restored = TestModel.model_validate_json(json_str)

        assert original.name == restored.name
        assert original.value == restored.value
