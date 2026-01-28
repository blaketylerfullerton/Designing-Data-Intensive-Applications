import struct
from schema import Schema, FieldType, SchemaRegistry
from encoder import MAGIC, HEADER_FORMAT, HEADER_SIZE

def decode_varint(data, offset):
    result = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise ValueError('unexpected end of data')
        byte = data[offset]
        result |= (byte & 0x7f) << shift
        offset += 1
        if not (byte & 0x80):
            break
        shift += 7
    return result, offset

def decode_string(data, offset):
    length, offset = decode_varint(data, offset)
    s = data[offset:offset + length].decode('utf-8')
    return s, offset + length

def decode_bytes(data, offset):
    length, offset = decode_varint(data, offset)
    b = data[offset:offset + length]
    return b, offset + length

class Decoder:
    def __init__(self, registry=None, target_schema=None):
        self.registry = registry
        self.target_schema = target_schema

    def decode(self, data):
        if len(data) < HEADER_SIZE:
            raise ValueError('data too short')

        magic, version, name_len = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])

        if magic != MAGIC:
            raise ValueError('invalid magic bytes')

        offset = HEADER_SIZE
        schema_name = data[offset:offset + name_len].decode('utf-8')
        offset += name_len

        source_schema = None
        if self.registry:
            source_schema = self.registry.get(schema_name, version)

        if self.target_schema:
            schema = self.target_schema
        elif source_schema:
            schema = source_schema
        else:
            raise ValueError(f'unknown schema: {schema_name} v{version}')

        result, _ = self._decode_fields(data, offset, schema, source_schema)
        return result

    def _decode_fields(self, data, offset, schema, source_schema=None):
        result = {}
        seen_tags = set()

        while offset < len(data):
            tag, offset = decode_varint(data, offset)
            if tag == 0:
                break

            seen_tags.add(tag)

            field_def = schema.get_field_by_tag(tag)
            source_field = source_schema.get_field_by_tag(tag) if source_schema else None

            if field_def is None:
                offset = self._skip_field(data, offset, source_field)
                continue

            value, offset = self._decode_value(data, offset, field_def)
            result[field_def.name] = value

        for field_def in schema.fields:
            if field_def.tag not in seen_tags:
                if field_def.default is not None:
                    result[field_def.name] = field_def.default
                elif field_def.required:
                    raise ValueError(f'missing required field: {field_def.name}')

        return result, offset

    def _decode_value(self, data, offset, field_def):
        field_type = field_def.field_type

        if field_type == FieldType.INT32:
            value = struct.unpack('>i', data[offset:offset + 4])[0]
            return value, offset + 4

        if field_type == FieldType.INT64:
            value = struct.unpack('>q', data[offset:offset + 8])[0]
            return value, offset + 8

        if field_type == FieldType.FLOAT32:
            value = struct.unpack('>f', data[offset:offset + 4])[0]
            return value, offset + 4

        if field_type == FieldType.FLOAT64:
            value = struct.unpack('>d', data[offset:offset + 8])[0]
            return value, offset + 8

        if field_type == FieldType.STRING:
            return decode_string(data, offset)

        if field_type == FieldType.BYTES:
            return decode_bytes(data, offset)

        if field_type == FieldType.BOOL:
            value = struct.unpack('>?', data[offset:offset + 1])[0]
            return value, offset + 1

        if field_type == FieldType.ARRAY:
            count, offset = decode_varint(data, offset)
            items = []
            element_type = field_def.element_type or FieldType.STRING
            temp_field = type('TempField', (), {'field_type': element_type, 'element_type': None, 'nested_schema': None})()
            for _ in range(count):
                item, offset = self._decode_value(data, offset, temp_field)
                items.append(item)
            return items, offset

        if field_type == FieldType.MAP:
            count, offset = decode_varint(data, offset)
            result = {}
            for _ in range(count):
                key, offset = decode_string(data, offset)
                value, offset = decode_string(data, offset)
                result[key] = value
            return result, offset

        if field_type == FieldType.NESTED:
            length, offset = decode_varint(data, offset)
            if field_def.nested_schema:
                nested_data = data[offset:offset + length]
                nested_result, _ = self._decode_fields(nested_data, 0, field_def.nested_schema)
                return nested_result, offset + length
            return {}, offset + length

        raise ValueError(f'unknown field type: {field_type}')

    def _skip_field(self, data, offset, source_field):
        if source_field is None:
            length, offset = decode_varint(data, offset)
            return offset + length

        field_type = source_field.field_type

        if field_type in (FieldType.INT32, FieldType.FLOAT32):
            return offset + 4
        if field_type in (FieldType.INT64, FieldType.FLOAT64):
            return offset + 8
        if field_type == FieldType.BOOL:
            return offset + 1
        if field_type in (FieldType.STRING, FieldType.BYTES):
            length, offset = decode_varint(data, offset)
            return offset + length
        if field_type == FieldType.ARRAY:
            count, offset = decode_varint(data, offset)
            temp_field = type('TempField', (), {'field_type': source_field.element_type or FieldType.STRING, 'element_type': None, 'nested_schema': None})()
            for _ in range(count):
                _, offset = self._decode_value(data, offset, temp_field)
            return offset
        if field_type == FieldType.MAP:
            count, offset = decode_varint(data, offset)
            for _ in range(count):
                _, offset = decode_string(data, offset)
                _, offset = decode_string(data, offset)
            return offset
        if field_type == FieldType.NESTED:
            length, offset = decode_varint(data, offset)
            return offset + length

        length, offset = decode_varint(data, offset)
        return offset + length

def decode(data, registry=None, target_schema=None):
    decoder = Decoder(registry, target_schema)
    return decoder.decode(data)


