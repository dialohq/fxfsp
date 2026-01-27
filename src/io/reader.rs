use std::io::Write;

use crate::error::FxfspError;
use crate::reader::{IoPhase, IoReader};

/// A decorator that wraps any [`IoReader`] and logs I/O operations to a CSV file.
pub struct InstrumentedReader<R> {
    inner: R,
    io_log: std::io::BufWriter<std::fs::File>,
    remaining: usize,
}

impl<R> InstrumentedReader<R> {
    /// Wrap `inner` with CSV logging to the given file path.
    pub fn new(inner: R, log_path: &str, limit: usize) -> Result<Self, FxfspError> {
        let f = std::fs::File::create(log_path).map_err(FxfspError::Io)?;
        let mut w = std::io::BufWriter::new(f);
        writeln!(w, "phase,offset,len").map_err(FxfspError::Io)?;
        Ok(Self {
            inner,
            io_log: w,
            remaining: limit,
        })
    }

    fn log_read(&mut self, phase: IoPhase, offset: u64, len: usize) {
        if self.remaining == 0 {
            return;
        }
        let _ = writeln!(self.io_log, "{},{},{}", phase, offset, len);
        self.remaining -= 1;
    }
}

impl<R: IoReader> IoReader for InstrumentedReader<R> {
    fn read_at(&mut self, offset: u64, len: usize, phase: IoPhase) -> Result<&[u8], FxfspError> {
        self.log_read(phase, offset, len);
        self.inner.read_at(offset, len, phase)
    }

    fn coalesced_read_batch<T: Copy, F>(
        &mut self,
        requests: &[(u64, usize, T)],
        on_complete: F,
        phase: IoPhase,
    ) -> Result<(), FxfspError>
    where
        F: FnMut(&[u8], T) -> Result<(), FxfspError>,
    {
        for &(offset, len, _) in requests {
            self.log_read(phase, offset, len);
        }
        self.inner.coalesced_read_batch(requests, on_complete, phase)
    }
}

/// Runtime choice between a bare reader and an instrumented one.
///
/// Avoids dynamic dispatch while allowing the decision to be made at runtime
/// (e.g. based on environment variables).
pub enum MaybeInstrumented<R> {
    Bare(R),
    Instrumented(InstrumentedReader<R>),
}

impl<R> MaybeInstrumented<R> {
    /// Construct from environment variables.
    ///
    /// If `FXFSP_IO_LOG` is set, wraps `inner` with CSV logging.
    /// `FXFSP_IO_LOG_LIMIT` optionally caps the number of logged operations.
    pub fn from_env(inner: R) -> Result<Self, FxfspError> {
        if let Ok(path) = std::env::var("FXFSP_IO_LOG") {
            let limit = std::env::var("FXFSP_IO_LOG_LIMIT")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(usize::MAX);
            Ok(Self::Instrumented(InstrumentedReader::new(inner, &path, limit)?))
        } else {
            Ok(Self::Bare(inner))
        }
    }
}

impl<R: IoReader> IoReader for MaybeInstrumented<R> {
    fn read_at(&mut self, offset: u64, len: usize, phase: IoPhase) -> Result<&[u8], FxfspError> {
        match self {
            Self::Bare(r) => r.read_at(offset, len, phase),
            Self::Instrumented(r) => r.read_at(offset, len, phase),
        }
    }

    fn coalesced_read_batch<T: Copy, F>(
        &mut self,
        requests: &[(u64, usize, T)],
        on_complete: F,
        phase: IoPhase,
    ) -> Result<(), FxfspError>
    where
        F: FnMut(&[u8], T) -> Result<(), FxfspError>,
    {
        match self {
            Self::Bare(r) => r.coalesced_read_batch(requests, on_complete, phase),
            Self::Instrumented(r) => r.coalesced_read_batch(requests, on_complete, phase),
        }
    }
}
