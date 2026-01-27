use thiserror::Error;

#[derive(Error, Debug)]
pub enum FxfspError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Bad magic number in {0}")]
    BadMagic(&'static str),
    #[error("Parse error: {0}")]
    Parse(&'static str),
    #[error("CRC mismatch in {0}")]
    CrcMismatch(&'static str),
    /// Scan was stopped early by the callback (not a real error).
    #[error("scan stopped by callback")]
    Stopped,
}
