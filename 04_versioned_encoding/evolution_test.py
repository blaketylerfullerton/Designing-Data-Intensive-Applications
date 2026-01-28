from schema import Schema, FieldType, SchemaRegistry, check_compatibility
from encoder import encode
from decoder import decode

registry = SchemaRegistry()

user_v1 = Schema('User', 1)
user_v1.add_field(1, 'id', FieldType.INT64, required=True)
user_v1.add_field(2, 'name', FieldType.STRING, required=True)
user_v1.add_field(3, 'email', FieldType.STRING)
registry.register(user_v1)

user_v2 = Schema('User', 2)
user_v2.add_field(1, 'id', FieldType.INT64, required=True)
user_v2.add_field(2, 'name', FieldType.STRING, required=True)
user_v2.add_field(3, 'email', FieldType.STRING)
user_v2.add_field(4, 'age', FieldType.INT32, default=0)
user_v2.add_field(5, 'tags', FieldType.ARRAY, element_type=FieldType.STRING, default=[])
registry.register(user_v2)

user_v3 = Schema('User', 3)
user_v3.add_field(1, 'id', FieldType.INT64, required=True)
user_v3.add_field(2, 'name', FieldType.STRING, required=True)
user_v3.add_field(4, 'age', FieldType.INT32, default=0)
user_v3.add_field(5, 'tags', FieldType.ARRAY, element_type=FieldType.STRING, default=[])
user_v3.add_field(6, 'metadata', FieldType.MAP, default={})
registry.register(user_v3)

address_schema = Schema('Address', 1)
address_schema.add_field(1, 'street', FieldType.STRING)
address_schema.add_field(2, 'city', FieldType.STRING)
address_schema.add_field(3, 'zip', FieldType.STRING)

user_v4 = Schema('User', 4)
user_v4.add_field(1, 'id', FieldType.INT64, required=True)
user_v4.add_field(2, 'name', FieldType.STRING, required=True)
user_v4.add_field(4, 'age', FieldType.INT32, default=0)
user_v4.add_field(5, 'tags', FieldType.ARRAY, element_type=FieldType.STRING, default=[])
user_v4.add_field(6, 'metadata', FieldType.MAP, default={})
user_v4.add_field(7, 'address', FieldType.NESTED, nested_schema=address_schema)
registry.register(user_v4)

def test_forward_compatibility():
    data_v1 = {'id': 12345, 'name': 'alice', 'email': 'alice@example.com'}
    encoded = encode(user_v1, data_v1)
    decoded = decode(encoded, registry, user_v2)
    assert decoded['id'] == 12345
    assert decoded['name'] == 'alice'
    assert decoded['email'] == 'alice@example.com'
    assert decoded['age'] == 0
    assert decoded['tags'] == []
    print(f'forward compat v1->v2: {decoded}')

def test_backward_compatibility():
    data_v2 = {'id': 67890, 'name': 'bob', 'email': 'bob@example.com', 'age': 30, 'tags': ['admin', 'user']}
    encoded = encode(user_v2, data_v2)
    decoded = decode(encoded, registry, user_v1)
    assert decoded['id'] == 67890
    assert decoded['name'] == 'bob'
    assert decoded['email'] == 'bob@example.com'
    assert 'age' not in decoded
    print(f'backward compat v2->v1: {decoded}')

def test_field_removal():
    data_v2 = {'id': 11111, 'name': 'charlie', 'email': 'charlie@example.com', 'age': 25, 'tags': []}
    encoded = encode(user_v2, data_v2)
    decoded = decode(encoded, registry, user_v3)
    assert decoded['id'] == 11111
    assert decoded['name'] == 'charlie'
    assert 'email' not in decoded
    print(f'field removal v2->v3: {decoded}')

def test_nested_schema():
    data_v4 = {
        'id': 22222,
        'name': 'diana',
        'age': 28,
        'tags': ['premium'],
        'metadata': {'source': 'web'},
        'address': {'street': '123 main st', 'city': 'springfield', 'zip': '12345'}
    }
    encoded = encode(user_v4, data_v4)
    decoded = decode(encoded, registry, user_v4)
    assert decoded['address']['city'] == 'springfield'
    print(f'nested schema: {decoded}')

def test_compatibility_check():
    compat, issues = check_compatibility(user_v1, user_v2)
    print(f'v1->v2 compatible: {compat}, issues: {issues}')

    compat, issues = check_compatibility(user_v2, user_v3)
    print(f'v2->v3 compatible: {compat}, issues: {issues}')

    bad_schema = Schema('User', 99)
    bad_schema.add_field(1, 'id', FieldType.STRING)
    compat, issues = check_compatibility(user_v1, bad_schema)
    print(f'v1->bad compatible: {compat}, issues: {issues}')

def test_unknown_fields():
    data = {'id': 33333, 'name': 'eve', 'age': 35, 'tags': ['test'], 'metadata': {'key': 'value'}}
    encoded = encode(user_v3, data)
    decoded = decode(encoded, registry, user_v1)
    print(f'unknown fields ignored: {decoded}')

if __name__ == '__main__':
    test_forward_compatibility()
    test_backward_compatibility()
    test_field_removal()
    test_nested_schema()
    test_compatibility_check()
    test_unknown_fields()
    print('all tests passed')


