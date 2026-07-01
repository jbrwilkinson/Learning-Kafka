//! Example Kafka consumer for learning purposes.
//!
//! Subscribes to a topic as part of a consumer group, logs every message it
//! receives (partition/offset/key), and periodically reports consumer lag
//! per partition by comparing committed offsets against the partition's
//! high watermark. `PROCESSING_DELAY_MS` simulates slow processing so you
//! can watch lag build up on purpose.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;

struct LoggingContext;

impl ClientContext for LoggingContext {}

impl ConsumerContext for LoggingContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        log::info!("[REBALANCE] starting: {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        log::info!("[REBALANCE] finished: {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        if let Err(e) = result {
            log::warn!("Error committing offsets: {}", e);
        }
    }
}

type LoggingConsumer = StreamConsumer<LoggingContext>;

async fn report_lag(consumer: Arc<LoggingConsumer>, topic: String) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;

        let assignment = match consumer.assignment() {
            Ok(a) => a,
            Err(e) => {
                log::warn!("[LAG] failed to get assignment: {}", e);
                continue;
            }
        };
        if assignment.count() == 0 {
            continue;
        }

        let committed = match consumer.committed(Duration::from_secs(5)) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("[LAG] failed to fetch committed offsets: {}", e);
                continue;
            }
        };

        for elem in committed.elements() {
            let partition = elem.partition();
            let committed_offset = match elem.offset() {
                Offset::Offset(o) => o,
                _ => 0,
            };

            match consumer.fetch_watermarks(&topic, partition, Duration::from_secs(5)) {
                Ok((_low, high)) => {
                    let lag = (high - committed_offset).max(0);
                    log::info!(
                        "[LAG] partition={} committed_offset={} high_watermark={} lag={}",
                        partition,
                        committed_offset,
                        high,
                        lag
                    );
                }
                Err(e) => {
                    log::warn!("[LAG] failed to fetch watermarks for partition {}: {}", partition, e);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let brokers = env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into());
    let group_id = env::var("GROUP_ID").unwrap_or_else(|_| "learning-consumer-group".into());
    let topic = env::var("TOPIC").unwrap_or_else(|_| "events".into());
    let enable_auto_commit = env::var("ENABLE_AUTO_COMMIT").unwrap_or_else(|_| "false".into());
    let processing_delay_ms: u64 = env::var("PROCESSING_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);

    log::info!(
        "Starting consumer: brokers={} group_id={} topic={} enable_auto_commit={} processing_delay_ms={}",
        brokers, group_id, topic, enable_auto_commit, processing_delay_ms
    );

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.auto.commit", &enable_auto_commit)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create_with_context(LoggingContext)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic.as_str()])
        .expect("Failed to subscribe to topic");

    let consumer = Arc::new(consumer);
    let manual_commit = enable_auto_commit.to_lowercase() != "true";

    tokio::spawn(report_lag(Arc::clone(&consumer), topic.clone()));

    loop {
        match consumer.recv().await {
            Err(e) => log::warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    Some(Ok(s)) => s.to_string(),
                    Some(Err(e)) => {
                        log::warn!("Invalid UTF-8 payload: {}", e);
                        continue;
                    }
                    None => String::new(),
                };
                let key = m
                    .key()
                    .map(|k| String::from_utf8_lossy(k).to_string())
                    .unwrap_or_default();

                log::info!(
                    "Received partition={} offset={} key={} payload={}",
                    m.partition(),
                    m.offset(),
                    key,
                    payload
                );

                if processing_delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(processing_delay_ms)).await;
                }

                if manual_commit {
                    if let Err(e) = consumer.commit_message(&m, CommitMode::Async) {
                        log::warn!("Failed to commit offset: {}", e);
                    }
                }
            }
        }
    }
}
