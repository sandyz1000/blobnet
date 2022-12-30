//! Faster asynchronous I/O implementations.
//!
//! This module allows unsafe code to be used, which speeds up file I/O, mainly
//! due to creating uninitialized memory and calling [`libc::pread`].

#![allow(unsafe_code)]

use std::fs::File;
use std::future::Future;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use tokio::io::AsyncRead;
use tokio::task;

use crate::{BlobRange, ReadStream};

/// Stream data from a file, optionally at a range of bytes.
///
/// This is preferred over [`tokio::fs::File`] because that implementation has a
/// non-configurable 16 KiB intermediate buffer size, which makes large reads
/// extremely inefficient.
///
/// It also uses `pread`, resulting in one less `lseek` system call for reads in
/// the middle of a file. All read operations are buffered at 2 MiB.
pub(crate) fn file_reader(file: impl Into<Arc<File>>, range: BlobRange) -> ReadStream<'static> {
    let range = range.unwrap_or((0, u64::MAX));
    let buf_size = (range.1 - range.0).min(1 << 21) as usize;
    Box::pin(FileReader {
        file: file.into(),
        offset: range.0,
        end: range.1,
        state: FileReaderState::Idle(new_uninit(buf_size)),
    })
}

type UnsafeBuf = Box<[MaybeUninit<u8>]>;

/// Create a new, uninitialized buffer for reading from a file.
///
/// This is an equivalent replacement for the `Box::new_uninit_slice` function,
/// which is currently unstable.
fn new_uninit(n: usize) -> UnsafeBuf {
    use std::alloc::{alloc, handle_alloc_error, Layout};

    let layout = Layout::array::<MaybeUninit<u8>>(n)
        .expect("failed to create allocation layout for reading buf");
    // Safety: We check for allocation failure.
    let ptr = unsafe { alloc(layout) };
    if ptr.is_null() {
        handle_alloc_error(layout);
    }
    let slice = std::ptr::slice_from_raw_parts_mut(ptr.cast(), n);
    // Safety: We just allocated this memory, with the appropriate size.
    unsafe { Box::from_raw(slice) }
}

/// Assuming all the elements are initialized, get a slice to them.
///
/// This is copied from the `MaybeUninit::slice_assume_init_ref` function, which
/// is currently unstable.
unsafe fn slice_assume_init_ref(slice: &[MaybeUninit<u8>]) -> &[u8] {
    // Safety: See the documentation for the standard library version.
    unsafe { &*(slice as *const [MaybeUninit<u8>] as *const [u8]) }
}

struct FileReader {
    file: Arc<File>,
    offset: u64,
    end: u64,
    state: FileReaderState,
}

#[derive(Default)]
enum FileReaderState {
    Pending(task::JoinHandle<io::Result<(UnsafeBuf, usize)>>),
    Queued(UnsafeBuf, usize, usize),
    Idle(UnsafeBuf),
    #[default]
    None,
}

impl AsyncRead for FileReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        dst: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        loop {
            match std::mem::take(&mut this.state) {
                // There are immediate bytes to be read from the buffer.
                FileReaderState::Queued(buf, start, end) => {
                    assert!(start < end);
                    let size = std::cmp::min(dst.remaining(), end - start);
                    // Safety: We previously this range of bytes from the file, using the
                    // `libc::pread` function below.
                    dst.put_slice(unsafe { slice_assume_init_ref(&buf[start..start + size]) });
                    if start + size == end {
                        this.state = FileReaderState::Idle(buf);
                    } else {
                        this.state = FileReaderState::Queued(buf, start + size, end);
                    }
                    return Poll::Ready(Ok(()));
                }
                // Currently waiting on an I/O operation to complete.
                FileReaderState::Pending(mut handle) => {
                    let (buf, n) = match Pin::new(&mut handle).poll(cx) {
                        Poll::Ready(result) => result??,
                        Poll::Pending => {
                            this.state = FileReaderState::Pending(handle);
                            return Poll::Pending;
                        }
                    };
                    if n == 0 {
                        this.end = this.offset; // No more bytes to read.
                        this.state = FileReaderState::Idle(buf);
                    } else {
                        this.offset += n as u64;
                        this.state = FileReaderState::Queued(buf, 0, n);
                    }
                }
                // No ongoing I/O operations or pending bytes.
                FileReaderState::Idle(mut buf) => {
                    let offset = this.offset;
                    if offset >= this.end {
                        this.state = FileReaderState::Idle(buf);
                        return Poll::Ready(Ok(())); // Done with the file.
                    }
                    let file = Arc::clone(&this.file);
                    let read_len = (this.end - offset).min(buf.len() as u64) as usize;

                    this.state = FileReaderState::Pending(task::spawn_blocking(move || {
                        // Safety: The file is available, and we're the only thread reading from it.
                        // The unsafe buffer is allocated and has the proper length.
                        let result = unsafe {
                            libc::pread(
                                file.as_raw_fd(),
                                buf.as_mut_ptr().cast(),
                                read_len,
                                offset as i64,
                            )
                        };
                        if result < 0 {
                            Err(io::Error::last_os_error())
                        } else {
                            Ok::<_, io::Error>((buf, result as usize))
                        }
                    }));
                }
                FileReaderState::None => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Arc};

    use anyhow::Result;
    use tokio::task;

    use super::file_reader;
    use crate::read_to_vec;

    #[tokio::test]
    async fn test_file_reader() -> Result<()> {
        let file = Arc::new(tempfile::tempfile()?);

        let file2 = Arc::clone(&file);
        task::spawn_blocking(move || (&mut &*file2).write_all(b"hello world")).await??;

        let reader = file_reader(file, None);
        assert_eq!(read_to_vec(reader).await?, b"hello world");

        Ok(())
    }
}
