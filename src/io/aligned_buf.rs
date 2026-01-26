use aligned_vec::{AVec, ConstAlign};

/// Alignment required for O_DIRECT I/O (512 bytes covers all common block devices).
pub const IO_ALIGN: usize = 512;

pub type AlignedBuf = AVec<u8, ConstAlign<IO_ALIGN>>;

/// Create a new aligned buffer of `size` bytes, zeroed.
pub fn alloc_aligned(size: usize) -> AlignedBuf {
    AVec::from_iter(IO_ALIGN, std::iter::repeat_n(0u8, size))
}
