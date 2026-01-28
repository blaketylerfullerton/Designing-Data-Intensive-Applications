# Reliability Service

Fault tolerance patterns for building resilient distributed systems.

## Concepts

This module implements three key reliability patterns from Chapter 1 of DDIA:

- **Circuit Breaker** — Prevents cascading failures by failing fast when a service is unhealthy
- **Rate Limiter** — Protects services from being overwhelmed by too many requests
- **Retry with Backoff** — Handles transient failures with exponential backoff

## Files

| File | Description |
|------|-------------|
| `server.py` | HTTP server with configurable failure injection |
| `client.py` | Reliable client with circuit breaker, rate limiter, retries |
| `load_test.py` | Load generator for testing under various failure scenarios |

## Usage

Start the server:
```bash
python server.py
```

Run the client demo:
```bash
python client.py
```

Run load tests:
```bash
python load_test.py
```

## Circuit Breaker States

```
     success
        │
        ▼
    ┌───────┐  failures >= threshold   ┌──────┐
    │CLOSED │─────────────────────────▶│ OPEN │
    └───────┘                          └──────┘
        ▲                                  │
        │                      timeout expires
        │                                  │
        │     successes >= threshold   ┌───▼─────┐
        └──────────────────────────────│HALF_OPEN│
                                       └─────────┘
```

## Server Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Normal request (may fail based on config) |
| `/health` | GET | Health check |
| `/stats` | GET | Server statistics |
| `/config` | POST | Configure failure rate and latency |

## Configuration

Inject failures via POST to `/config`:
```bash
curl -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d '{"failure_rate": 0.3, "latency_ms": 100}'
```

## Load Test Scenarios

The load tester runs through several scenarios:

1. **Baseline** — No failures, measure normal latency
2. **10% failures** — Light failure rate
3. **50% failures** — Heavy failure rate  
4. **Added latency** — 100ms artificial delay
5. **Latency + failures** — Combined stress
6. **High load** — 200 req/s burst

## Key Parameters

```python
CircuitBreaker(
    failure_threshold=5,    # Failures before opening
    recovery_timeout=30.0,  # Seconds before trying again
    half_open_max=3         # Successes needed to close
)

RateLimiter(
    max_requests=100,       # Requests allowed
    window_seconds=1.0      # Per time window
)

RetryPolicy(
    max_retries=3,          # Retry attempts
    base_delay=0.1,         # Initial delay (seconds)
    max_delay=10.0,         # Maximum delay cap
    exponential=True        # Use exponential backoff
)
```


