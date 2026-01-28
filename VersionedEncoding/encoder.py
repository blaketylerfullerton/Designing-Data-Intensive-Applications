import struct
from schema import Schema, FieldType

MAGIC = b'VENC'
HEADER_FORMAT = '>4s H H'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

def encode_varint(value):
    result = []
    if value < 0:
        value = (1 << 64) + value
    while value > 0x7f:
        result.append((value & 0x7f) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)

def encode_string(s):
    encoded = s.encode('utf-8')
    return encode_varint(len(encoded)) + encoded

def encode_bytes(b):
    return encode_varint(len(b)) + b

class Encoder:
    def __init__(self, schema):
        self.schema = schema

    def encode(self, data):
        header = struct.pack(
            HEADER_FORMAT,
            MAGIC,
            self.schema.version,
            len(self.schema.name)
        )
        schema_name = self.schema.name.encode('utf-8')

        body = self._encode_fields(data)

        return header + schema_name + body

    def _encode_fields(self, data):
        result = bytearray()

        for field_def in self.schema.fields:
            if field_def.name not in data:
                if field_def.required:
                    raise ValueError(f'missing required field: {field_def.name}')
                continue

            value = data[field_def.name]
            if value is None:
                continue

            result.extend(encode_varint(field_def.tag))
            result.extend(self._encode_value(value, field_def.field_type, field_def))

        result.extend(encode_varint(0))

        return bytes(result)

    def _encode_value(self, value, field_type, field_def=None):
        if field_type == FieldType.INT32:
            return struct.pack('>i', value)

        if field_type == FieldType.INT64:
            return struct.pack('>q', value)

        if field_type == FieldType.FLOAT32:
            return struct.pack('>f', value)

        if field_type == FieldType.FLOAT64:
            return struct.pack('>d', value)

        if field_type == FieldType.STRING:
            return encode_string(value)

        if field_type == FieldType.BYTES:
            return encode_bytes(value)

        if field_type == FieldType.BOOL:
            return struct.pack('>?', value)

        if field_type == FieldType.ARRAY:
            result = bytearray()
            result.extend(encode_varint(len(value)))
            element_type = field_def.element_type if field_def else FieldType.STRING
            for item in value:
                result.extend(self._encode_value(item, element_type))
            return bytes(result)

        if field_type == FieldType.MAP:
            result = bytearray()
            result.extend(encode_varint(len(value)))
            for k, v in value.items():
                result.extend(encode_string(str(k)))
                result.extend(encode_string(str(v)))
            return bytes(result)

        if field_type == FieldType.NESTED:
            if field_def and field_def.nested_schema:
                nested_encoder = Encoder(field_def.nested_schema)
                nested_data = nested_encoder._encode_fields(value)
                return encode_varint(len(nested_data)) + nested_data
            else:
                return encode_bytes(b'')

        raise ValueError(f'unknown field type: {field_type}')

def encode(schema, data):
    encoder = Encoder(schema)
    return encoder.encode(data)


