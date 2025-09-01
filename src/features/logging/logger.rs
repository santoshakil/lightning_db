use crate::features::logging::config::{LoggingConfig, LogFormat, FileRotation};
use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex};
use std::io::Write;
use tracing::Level;
use tracing_subscriber::{
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter, Registry,
    fmt::{self, time::SystemTime, MakeWriter},
};
use tracing_appender::{non_blocking, rolling};
#[cfg(feature = "telemetry")]
use opentelemetry::global;
#[cfg(feature = "telemetry")]
use opentelemetry::KeyValue;
#[cfg(feature = "telemetry")]
use opentelemetry_sdk::{trace::{self, TracerProvider}, Resource};
#[cfg(feature = "telemetry")]
use tracing_opentelemetry;

static LOGGER_INITIALIZED: Lazy<Arc<Mutex<bool>>> = Lazy::new(|| Arc::new(Mutex::new(false)));

pub struct Logger {
    config: LoggingConfig,
    _guards: Vec<tracing_appender::non_blocking::WorkerGuard>,
    #[cfg(feature = "telemetry")]
    tracer_provider: Option<TracerProvider>,
}

impl Logger {
    pub fn new(config: LoggingConfig) -> Result<Self, Box<dyn std::error::Error>> {
        config.validate()?;
        
        let mut logger = Self {
            config,
            _guards: Vec::new(),
            #[cfg(feature = "telemetry")]
            tracer_provider: None,
        };
        
        logger.initialize()?;
        Ok(logger)
    }
    
    pub fn init_global(config: LoggingConfig) -> Result<(), Box<dyn std::error::Error>> {
        let mut initialized = LOGGER_INITIALIZED.lock().unwrap();
        if *initialized {
            return Ok(());
        }
        
        let _logger = Self::new(config)?;
        *initialized = true;
        Ok(())
    }
    
    fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Environment filter
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| {
                let level: Level = self.config.level.clone().into();
                EnvFilter::new(format!("lightning_db={},warn", level))
            });
        
        // Initialize the subscriber
        let registry = Registry::default().with(env_filter);
        
        // Simple approach: Just use the console layer for now
        // This avoids complex dynamic layer composition issues
        let console_layer = fmt::layer()
            .with_target(false)
            .with_thread_ids(true)
            .with_line_number(true);
            
        registry.with(console_layer).init();
        
        Ok(())
    }
    
    fn create_console_layer(&self) -> Box<dyn tracing_subscriber::Layer<Registry> + Send + Sync> {
        match self.config.format {
            LogFormat::Json => Box::new(fmt::layer()
                .with_timer(SystemTime)
                .with_target(true)
                .with_file(true)
                .with_line_number(true)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_span_events(fmt::format::FmtSpan::CLOSE)),
            LogFormat::Compact => Box::new(fmt::layer()
                .compact()
                .with_timer(SystemTime)
                .with_target(false)
                .with_file(false)
                .with_line_number(false)
                .with_thread_ids(false)
                .with_thread_names(true)),
            LogFormat::Pretty => Box::new(fmt::layer()
                .pretty()
                .with_timer(SystemTime)
                .with_target(true)
                .with_file(true)
                .with_line_number(true)
                .with_thread_ids(false)
                .with_thread_names(true)
                .with_span_events(fmt::format::FmtSpan::FULL)),
            LogFormat::Full => Box::new(fmt::layer()
                .with_timer(SystemTime)
                .with_target(true)
                .with_file(true)
                .with_line_number(true)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_span_events(fmt::format::FmtSpan::FULL)),
        }
    }
    
    fn create_file_layer(
        &self,
        file_config: &crate::features::logging::config::FileOutput,
    ) -> Result<(Box<dyn tracing_subscriber::Layer<Registry> + Send + Sync>, tracing_appender::non_blocking::WorkerGuard), Box<dyn std::error::Error>> {
        let file_appender = match &file_config.rotation {
            FileRotation::Hourly => rolling::hourly(&file_config.path, "lightning_db.log"),
            FileRotation::Daily => rolling::daily(&file_config.path, "lightning_db.log"),
            FileRotation::Size(_size) => {
                // For size-based rotation, we'll use daily for now
                // In a full implementation, you'd integrate with a log rotation library
                rolling::daily(&file_config.path, "lightning_db.log")
            }
        };
        
        let (non_blocking, guard) = non_blocking(file_appender);
        
        let layer = Box::new(fmt::layer()
            .with_writer(non_blocking)
            .with_timer(SystemTime)
            .with_target(true)
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_span_events(fmt::format::FmtSpan::CLOSE));
            
        Ok((layer, guard))
    }
    
    fn create_otel_layer(
        &mut self,
        jaeger_config: &crate::features::logging::config::JaegerConfig,
    ) -> Result<Box<dyn tracing_subscriber::Layer<Registry> + Send + Sync>, Box<dyn std::error::Error>> {
        #[cfg(feature = "telemetry")]
        let mut resource_attrs = vec![
            KeyValue::new("service.name", self.config.telemetry.service_name.clone()),
            KeyValue::new("service.version", self.config.telemetry.service_version.clone()),
            KeyValue::new("environment", self.config.telemetry.environment.clone()),
        ];
        
        #[cfg(feature = "telemetry")]
        for (key, value) in &self.config.telemetry.resource_attributes {
            resource_attrs.push(KeyValue::new(key.clone(), value.clone()));
        }
        
        #[cfg(not(feature = "telemetry"))]
        let resource_attrs: Vec<(&str, String)> = vec![];
        
        // Use new pipeline API instead of deprecated one
        #[cfg(feature = "telemetry")]
        let tracer = {
            opentelemetry_jaeger::new_collector_pipeline()
                .with_service_name(&jaeger_config.service_name)
                .with_endpoint(&jaeger_config.endpoint)
                .with_trace_config(
                    trace::config().with_resource(Resource::new(resource_attrs))
                )
                .install_batch(opentelemetry_sdk::runtime::Tokio)?
        };
        
        #[cfg(not(feature = "telemetry"))]
        let tracer = {
            return Err("Telemetry feature not enabled".into());
        };
            
        #[cfg(feature = "telemetry")]
        {
            self.tracer_provider = Some(tracer.provider().unwrap().clone());
            Ok(Box::new(tracing_opentelemetry::layer().with_tracer(tracer)))
        }
        #[cfg(not(feature = "telemetry"))]
        {
            // Unreachable due to early return above, but needed for type checking
            Err("Telemetry feature not enabled".into())
        }
    }
}

impl Drop for Logger {
    fn drop(&mut self) {
        #[cfg(feature = "telemetry")]
        {
            if let Some(provider) = self.tracer_provider.take() {
                let _ = provider.force_flush();
            }
            global::shutdown_tracer_provider();
        }
    }
}

// Custom writer for redaction
pub struct RedactingWriter<W> {
    inner: W,
    redactor: crate::features::logging::redaction::Redactor,
}

impl<W> RedactingWriter<W> {
    pub fn new(inner: W, redactor: crate::features::logging::redaction::Redactor) -> Self {
        Self { inner, redactor }
    }
}

impl<W: Write> Write for RedactingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Ok(s) = std::str::from_utf8(buf) {
            let redacted = self.redactor.redact(s);
            self.inner.write(redacted.as_bytes())
        } else {
            self.inner.write(buf)
        }
    }
    
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Write> MakeWriter<'_> for RedactingWriter<W> 
where
    W: Clone + Send + Sync + 'static,
{
    type Writer = Self;
    
    fn make_writer(&'_ self) -> <RedactingWriter<W> as MakeWriter<'_>>::Writer {
        Self {
            inner: self.inner.clone(),
            redactor: self.redactor.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    
    #[test]
    fn test_logger_initialization() {
        let config = LoggingConfig::default();
        let logger = Logger::new(config);
        assert!(logger.is_ok());
    }
    
    #[test]
    fn test_config_validation() {
        let mut config = LoggingConfig::default();
        assert!(config.validate().is_ok());
        
        config.sampling.trace_sample_rate = 2.0;
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_redacting_writer() {
        let buffer = Arc::new(Mutex::new(Vec::<u8>::new()));
        let _writer = RedactingWriter::new(
            buffer.clone(),
            crate::features::logging::redaction::Redactor::new(vec!["secret".to_string()], "[REDACTED]".to_string()),
        );
        
        // This would test the redacting functionality once the redaction module is implemented
    }
}
