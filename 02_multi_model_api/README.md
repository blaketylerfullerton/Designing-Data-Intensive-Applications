# Multi-Model API

A unified API layer over three different data models: relational, document, and graph.

## Concepts

From Chapter 2 of DDIA, this module explores how the same data can be represented in different models, each with its own strengths:

- **Relational** — Tables with rows and foreign keys (SQLite)
- **Document** — Self-contained JSON documents (filesystem)
- **Graph** — Nodes and edges for relationship traversal (in-memory)

## Files

| File | Description |
|------|-------------|
| `api.py` | Unified HTTP API that works with any backend |
| `relational_store.py` | SQLite-based relational storage |
| `document_store.py` | JSON file-based document storage |
| `graph_store.py` | In-memory graph with traversal support |

## Usage

Start the API with a specific backend:
```bash
python api.py relational
python api.py document
python api.py graph
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/users` | GET | Query users with filters |
| `/users` | POST | Create a user |
| `/users/{id}` | GET | Get user by ID |
| `/users/{id}` | PUT | Update user |
| `/users/{id}` | DELETE | Delete user |
| `/users/{id}/relationships` | GET | Get user's relationships |
| `/users/{id}/relationships` | POST | Create a relationship |

## Example Requests

Create a user:
```bash
curl -X POST http://localhost:8081/users \
  -H "Content-Type: application/json" \
  -d '{"id": "alice", "name": "Alice", "email": "alice@example.com"}'
```

Add a relationship:
```bash
curl -X POST http://localhost:8081/users/alice/relationships \
  -H "Content-Type: application/json" \
  -d '{"to": "bob", "type": "follows"}'
```

Query relationships:
```bash
curl "http://localhost:8081/users/alice/relationships?type=follows"
```

## Storage Comparison

| Feature | Relational | Document | Graph |
|---------|------------|----------|-------|
| Schema | Fixed columns | Flexible JSON | Node properties |
| Joins | SQL JOINs | Application-level | Edge traversal |
| Queries | SQL WHERE | Key/value scan | BFS/DFS |
| Best for | Structured data | Varying schemas | Relationships |

## Graph-Specific Operations

The graph store supports traversal operations:

```python
# Traverse relationships up to depth=2
graph.traverse(start_id='alice', rel_type='follows', depth=2)

# Find shortest path between nodes
graph.shortest_path(from_id='alice', to_id='charlie', rel_type='follows')
```

## Data Storage

- **Relational**: `relational.db` (SQLite file)
- **Document**: `document_data/` directory with JSON files
- **Graph**: `graph_data.json` (persisted in-memory structure)

