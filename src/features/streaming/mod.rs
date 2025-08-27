pub mod cdc_engine;
pub mod stream_processor;
pub mod kafka_connector;
pub mod debezium;
pub mod event_sourcing;
pub mod change_stream;
pub mod binlog_reader;
pub mod watermark;

pub use cdc_engine::{CDCEngine, ChangeEvent, CaptureMode};
pub use stream_processor::{StreamProcessor, StreamOperator, WindowType};
pub use kafka_connector::{KafkaProducer, KafkaConsumer, KafkaConfig};
pub use debezium::{DebeziumConnector, SourceConnector, SinkConnector};
pub use event_sourcing::{EventStore, EventStream, AggregateRoot};
pub use change_stream::{ChangeStream, ChangeStreamOptions, ResumeToken};
pub use binlog_reader::{BinlogReader, BinlogEvent, BinlogPosition};
pub use watermark::{WatermarkGenerator, WatermarkStrategy, EventTimeExtractor};