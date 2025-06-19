use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid database file")]
    InvalidDatabase,

    #[error("Corrupted page")]
    CorruptedPage,

    #[error("Invalid page ID")]
    InvalidPageId,

    #[error("Page not found")]
    PageNotFound,

    #[error("Key not found")]
    KeyNotFound,

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Serialization error")]
    Serialization,

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Decompression error: {0}")]
    Decompression(String),

    #[error("Index error: {0}")]
    Index(String),

    #[error("Memory allocation error")]
    Memory,

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Timeout error: {0}")]
    Timeout(String),
    
    #[error("Generic error: {0}")]
    Generic(String),
}

impl Error {
    pub fn error_code(&self) -> i32 {
        match self {
            Error::Io(_) => -1,
            Error::InvalidDatabase => -2,
            Error::CorruptedPage => -3,
            Error::InvalidPageId => -4,
            Error::PageNotFound => -5,
            Error::KeyNotFound => -6,
            Error::Transaction(_) => -7,
            Error::Serialization => -8,
            Error::Compression(_) => -9,
            Error::Decompression(_) => -10,
            Error::Index(_) => -11,
            Error::Memory => -12,
            Error::Config(_) => -13,
            Error::Storage(_) => -14,
            Error::Timeout(_) => -15,
            Error::Generic(_) => -99,
        }
    }
}
