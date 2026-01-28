from enum import IntEnum
from dataclasses import dataclass, field
from typing import Any, Optional

class FieldType(IntEnum):
    INT32 = 1
    INT64 = 2
    FLOAT32 = 3
    FLOAT64 = 4
    STRING = 5
    BYTES = 6
    BOOL = 7
    ARRAY = 8
    MAP = 9
    NESTED = 10

@dataclass
class FieldDef:
    tag: int
    name: str
    field_type: FieldType
    required: bool = False
    default: Any = None
    element_type: Optional[FieldType] = None
    nested_schema: Optional['Schema'] = None

@dataclass
class Schema:
    name: str
    version: int
    fields: list = field(default_factory=list)

    def add_field(self, tag, name, field_type, required=False, default=None,
                  element_type=None, nested_schema=None):
        f = FieldDef(
            tag=tag,
            name=name,
            field_type=field_type,
            required=required,
            default=default,
            element_type=element_type,
            nested_schema=nested_schema
        )
        self.fields.append(f)
        return self

    def get_field_by_tag(self, tag):
        for f in self.fields:
            if f.tag == tag:
                return f
        return None

    def get_field_by_name(self, name):
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def validate(self, data):
        for f in self.fields:
            if f.required and f.name not in data:
                raise ValueError(f'missing required field: {f.name}')

            if f.name in data:
                value = data[f.name]
                if not self._type_check(value, f.field_type, f.element_type):
                    raise TypeError(f'invalid type for field {f.name}')

        return True

    def _type_check(self, value, field_type, element_type=None):
        if value is None:
            return True

        type_map = {
            FieldType.INT32: int,
            FieldType.INT64: int,
            FieldType.FLOAT32: float,
            FieldType.FLOAT64: float,
            FieldType.STRING: str,
            FieldType.BYTES: bytes,
            FieldType.BOOL: bool,
        }

        if field_type in type_map:
            return isinstance(value, type_map[field_type])

        if field_type == FieldType.ARRAY:
            if not isinstance(value, list):
                return False
            if element_type:
                return all(self._type_check(v, element_type) for v in value)
            return True

        if field_type == FieldType.MAP:
            return isinstance(value, dict)

        if field_type == FieldType.NESTED:
            return isinstance(value, dict)

        return False

class SchemaRegistry:
    def __init__(self):
        self.schemas = {}

    def register(self, schema):
        key = (schema.name, schema.version)
        self.schemas[key] = schema
        return schema

    def get(self, name, version):
        return self.schemas.get((name, version))

    def get_latest(self, name):
        matching = [(v, s) for (n, v), s in self.schemas.items() if n == name]
        if not matching:
            return None
        return max(matching, key=lambda x: x[0])[1]

    def get_versions(self, name):
        return sorted([v for (n, v) in self.schemas.keys() if n == name])

def check_compatibility(old_schema, new_schema):
    issues = []

    old_tags = {f.tag: f for f in old_schema.fields}
    new_tags = {f.tag: f for f in new_schema.fields}

    for tag, old_field in old_tags.items():
        if tag in new_tags:
            new_field = new_tags[tag]
            if old_field.field_type != new_field.field_type:
                issues.append(f'type change for tag {tag}: {old_field.field_type} -> {new_field.field_type}')

    for tag, new_field in new_tags.items():
        if tag not in old_tags and new_field.required and new_field.default is None:
            issues.append(f'new required field without default: tag {tag} ({new_field.name})')

    return len(issues) == 0, issues


