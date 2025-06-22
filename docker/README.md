# Docker Testing Infrastructure

This directory contains Docker Compose configurations for testing the PulsarClient Swift package.

## Available Configurations

1. **Standalone Mode** (`docker-compose.yml`)
   - Single Pulsar instance for basic testing
   - Includes Toxiproxy for network failure simulation
   - Optional Pulsar Manager UI

2. **Cluster Mode** (`docker-compose.cluster.yml`)
   - Full Pulsar cluster with Zookeeper, BookKeeper, and Broker
   - For testing distributed scenarios

3. **Authentication Mode** (`docker-compose.auth.yml`)
   - Pulsar with token authentication enabled
   - For testing authentication scenarios

## Quick Start

### Start Pulsar for Testing
```bash
# Standalone mode (default)
../scripts/test-env.sh start

# Cluster mode
../scripts/test-env.sh start --cluster

# With authentication
../scripts/test-env.sh start --auth

# With UI
../scripts/test-env.sh start --ui
```

### Stop Pulsar
```bash
../scripts/test-env.sh stop
```

### View Logs
```bash
../scripts/test-env.sh logs
```

### Clean Up
```bash
../scripts/test-env.sh clean
```

## Connection Details

### Standalone Mode
- Pulsar Broker: `pulsar://localhost:6650`
- Admin API: `http://localhost:8080`
- Toxiproxy Pulsar: `pulsar://localhost:16650`
- Toxiproxy API: `http://localhost:8474`
- Pulsar Manager UI: `http://localhost:9527` (if enabled)

### Authentication Mode
- Pulsar Broker: `pulsar://localhost:6651`
- Admin API: `http://localhost:8081`
- Tokens are displayed when environment starts

## Testing with Toxiproxy

Toxiproxy allows simulating network failures:

```bash
# Create proxy (done automatically)
curl -X POST http://localhost:8474/proxies \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "pulsar",
    "listen": "0.0.0.0:16650",
    "upstream": "pulsar-standalone:6650"
  }'

# Disable connection
curl -X POST http://localhost:8474/proxies/pulsar \
  -H 'Content-Type: application/json' \
  -d '{"enabled": false}'

# Re-enable connection
curl -X POST http://localhost:8474/proxies/pulsar \
  -H 'Content-Type: application/json' \
  -d '{"enabled": true}'
```

## Environment Variables

- `PULSAR_MEM`: JVM memory settings (default: `-Xms512m -Xmx512m`)
- `PULSAR_GC`: JVM GC settings (default: `-XX:+UseG1GC`)
- `PULSAR_STANDALONE_USE_ZOOKEEPER`: Use Zookeeper in standalone mode (default: `true`)

## Troubleshooting

1. **Container won't start**: Check Docker daemon is running
2. **Port conflicts**: Ensure ports 6650, 8080, etc. are not in use
3. **Health check failures**: Increase timeout in `test-env.sh`
4. **Authentication issues**: Check token is properly retrieved from container