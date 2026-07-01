# Learning Kafka

A local, disposable Kafka environment for learning about configuration,
partitions, consumer lag, and debugging.

## What's in here

- **3-broker Kafka cluster** (`kafka1`/`kafka2`/`kafka3`) running in KRaft
  mode (no ZooKeeper) via `confluentinc/cp-kafka`. Topics are created with
  `replication-factor=3` so you can see leader election and ISR changes
  when a broker goes down.
- **`kafka-init`** — a one-shot container that creates two topics and exits:
  - `events` — 6 partitions, replication factor 3, `min.insync.replicas=2`
  - `events.dlq` — 3 partitions, replication factor 3
- **`kafka-ui`** ([localhost:8080](http://localhost:8080)) — a web UI for
  browsing topics, partitions, messages, and consumer groups.
- **`producer/`** — a Python producer (`confluent-kafka`) that generates
  synthetic user events, keyed by `user_id`, with all the interesting
  producer configs (acks, batching, compression, idempotence) exposed as
  env vars.
- **`consumer/`** — a Rust consumer (`rdkafka`) in a consumer group that
  logs every message it processes and reports per-partition lag every 10
  seconds, comparing committed offsets against each partition's high
  watermark.

## Getting started

```text
docker compose up -d --build
```

Watch it happen:

```text
docker compose logs -f producer consumer
```

Open the UI at [http://localhost:8080](http://localhost:8080) to browse the
`events` topic, its partitions, and the `learning-consumer-group` consumer
group.

Tear down (including data volumes):

```text
docker compose down -v
```

## Debugging commands

These run the same CLI tools the Kafka UI wraps, straight from a broker
container — useful for getting comfortable with them directly:

```text
# List topics
docker compose exec kafka1 kafka-topics --bootstrap-server kafka1:9092 --list

# Describe a topic: partitions, leaders, replicas, ISR
docker compose exec kafka1 kafka-topics --bootstrap-server kafka1:9092 --describe --topic events

# Describe the consumer group: per-partition current offset, log-end offset, and lag
docker compose exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:9092 \
  --describe --group learning-consumer-group

# Tail the topic directly, bypassing the consumer group
docker compose exec kafka1 kafka-console-consumer --bootstrap-server kafka1:9092 \
  --topic events --from-beginning
```

## Things to try

**Partitioning and ordering**
The producer keys every message by `user_id`, so all events for a given
user always land on the same partition (Kafka hashes the key to pick the
partition). Watch the producer logs — same key, same partition, every
time. Change `KEY_CARDINALITY` in `docker-compose.yml` (currently 20) and
restart the producer to see how key space size affects partition balance.

**Consumer lag**
`PROCESSING_DELAY_MS` (default 200ms) simulates slow processing in the
consumer. Raise it (e.g. to 2000) and watch the `[LAG]` log lines climb —
then cross-check the numbers against
`kafka-consumer-groups --describe` above; they should match.

**Rebalancing**
Scale the consumer group up:

```text
docker compose up -d --scale consumer=3
```

Watch the `[REBALANCE]` log lines as partitions get reassigned across the
three instances. Since `events` has 6 partitions, 3 consumers means 2
partitions each. Try `--scale consumer=7` — one consumer will sit idle,
since Kafka can't assign more consumers than partitions within one group.

**Broker failure and ISR**
Stop a broker and watch what happens to leadership:

```text
docker compose stop kafka2
docker compose exec kafka1 kafka-topics --bootstrap-server kafka1:9092 --describe --topic events
```

Partitions led by `kafka2` will fail over to another replica, and the ISR
list will shrink. Bring it back with `docker compose start kafka2` and
watch it rejoin the ISR once it catches up.

**Producer config tuning**
Edit the `producer` service's environment in `docker-compose.yml` and
`docker compose up -d --build producer` to see the effect:
- `ACKS=1` vs `all` — durability vs latency trade-off
- `LINGER_MS` / `BATCH_SIZE` — batching for throughput vs per-message latency
- `COMPRESSION_TYPE` — try `none`, `gzip`, `lz4`, `zstd`
- `ENABLE_IDEMPOTENCE=false` — see why idempotence requires `acks=all`

**Manual vs auto commit**
Set `ENABLE_AUTO_COMMIT=true` on the consumer and restart it. With manual
commit (the default here), the consumer only commits after "processing"
(the simulated delay) completes — kill the consumer mid-delay
(`docker compose kill consumer`) and restart it; messages it hadn't
committed yet will be redelivered.
