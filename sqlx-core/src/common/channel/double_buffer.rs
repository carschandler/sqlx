use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use crossbeam_utils::CachePadded;


use futures_util::task::AtomicWaker;

pub struct Sender<T> {
    shared: Arc<BufferShared<T>>,
    state: BufferState,
}

pub struct Receiver<T> {
    shared: Arc<BufferShared<T>>,
    state: BufferState,
}

struct BufferState {
    selected: SelectedBuffer,
    locked: bool,
    swapped: bool,
}

struct BufferShared<T> {
    // Ensure each field is in its own cache line.
    header: CachePadded<Header>,
    front: CachePadded<Mutex<VecDeque<T>>>,
    back: CachePadded<Mutex<VecDeque<T>>>,
}

#[derive(Debug)]
struct Header {
    closed: AtomicBool,

    locked_front: AtomicBool,
    locked_back: AtomicBool,

    waiting_front: AtomicWaker,
    waiting_back: AtomicWaker,

    buffer_capacity: usize,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum SelectedBuffer {
    Front,
    Back,
}

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let buffer_capacity = capacity / 2;
    assert_ne!(buffer_capacity, 0, "capacity / 2 must not be zero");

    // `Sender` starts out holding the lock to the front buffer.
    // `Receiver` starts out _wanting_ the lock to the front buffer.
    let shared = Arc::new(BufferShared {
        header: CachePadded::new(Header {
            closed: AtomicBool::new(false),
            locked_front: AtomicBool::new(true),
            locked_back: AtomicBool::new(false),
            waiting_front: AtomicWaker::new(),
            waiting_back: AtomicWaker::new(),
            buffer_capacity,
        }),
        front: CachePadded::new(Mutex::new(VecDeque::with_capacity(buffer_capacity))),
        back: CachePadded::new(Mutex::new(VecDeque::with_capacity(buffer_capacity))),
    });

    (
        Sender {
            shared: shared.clone(),
            state: BufferState {
                selected: SelectedBuffer::Front,
                locked: true,
                swapped: false,
            },
        },
        Receiver {
            shared: shared.clone(),
            state: BufferState {
                selected: SelectedBuffer::Front,
                locked: false,
                swapped: false,
            },
        }
    )
}

impl<T> Sender<T> {
    fn poll_buf<'shared>(state: &mut BufferState, shared: &'shared BufferShared<T>, cx: &mut Context<'_>) -> Poll<Option<MutexGuard<'shared, VecDeque<T>>>> {
        let buffer_capacity = shared.header.buffer_capacity;

        for _ in 0..1 {
            if shared.header.is_closed() {
                return Poll::Ready(None);
            }

            let Some(buf) = state.try_lock(&shared) else {
                break;
            };

            if buf.len() < buffer_capacity {
                state.swapped = false;
                return Poll::Ready(Some(buf));
            }

            state.release_lock(&shared.header);

            if !state.swapped {
                state.swap_buffers(&shared.header);
            }
        }

        shared.header.waiting(state.selected).register(cx.waker());
        Poll::Pending
    }

    pub async fn send(&mut self, val: T) -> Result<(), T> {
        let maybe_buf = std::future::poll_fn(|cx| Self::poll_buf(&mut self.state, &self.shared, cx))
            .await;

        let Some(mut buf) = maybe_buf else {
            return Err(val);
        };

        buf.push_back(val);

        Ok(())
    }
}

/// Closes the channel.
///
/// The receiver may continue to read messages until the channel is drained.
impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.state.release_lock(&self.shared.header);
        self.shared.header.close();
    }
}

impl<T> Receiver<T> {
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        for _ in 0..1 {
            if let Some(mut buf) = self.state.try_lock(&self.shared) {
                if let Some(item) = buf.pop_front() {
                    self.state.swapped = false;
                    return Poll::Ready(Some(item));
                }

                if !self.state.swapped {
                    self.state.swap_buffers(&self.shared.header);
                }
            }
        }

        if self.shared.header.is_closed() {
            return Poll::Ready(None);
        }

        self.shared.header.waiting(self.state.selected).register(cx.waker());
        Poll::Pending
    }

    pub async fn recv(&mut self) -> Option<T> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Unlike
        self.shared.header.close();
    }
}

impl BufferState {
    fn try_lock<'a, T>(&mut self, shared: &'a BufferShared<T>) -> Option<MutexGuard<'a, VecDeque<T>>> {
        if !self.locked {
            shared.header.lock_status(self.selected)
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .ok()?;

            self.locked = true;
        }

        shared.buffer(self.selected)
            .try_lock()
            .map(Some)
            .unwrap_or_else(|e| {
                match e {
                    TryLockError::Poisoned(_) => {
                        shared.header.close();
                        None
                    }
                    TryLockError::WouldBlock => {
                        panic!("BUG: selected buffer ({:?}) should not be locked: {:?}", self.selected, shared.header)
                    }
                }
            })
    }

    fn release_lock(&mut self, header: &Header) {
        if self.locked {
            header.lock_status(self.selected)
                .store(false, Ordering::Release);

            self.locked = false;

            header.waiting(self.selected).wake();
        }
    }

    fn swap_buffers(&mut self, header: &Header) {
        self.release_lock(header);
        self.selected = self.selected.next();
        self.swapped = true;
    }
}

impl Header {
    fn close(&self) {
        self.closed.store(true, Ordering::Release);

        self.waiting_front.wake();
        self.waiting_back.wake();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }


    fn lock_status(&self, buffer: SelectedBuffer) -> &AtomicBool {
        match buffer {
            SelectedBuffer::Front => &self.locked_front,
            SelectedBuffer::Back => &self.locked_back,
        }
    }

    fn waiting(&self, buffer: SelectedBuffer) -> &AtomicWaker {
        match buffer {
            SelectedBuffer::Front => &self.waiting_front,
            SelectedBuffer::Back => &self.waiting_back,
        }
    }
}

impl<T> BufferShared<T> {
    fn buffer(&self, buffer: SelectedBuffer) -> &Mutex<VecDeque<T>> {
        match buffer {
            SelectedBuffer::Front => &self.front,
            SelectedBuffer::Back => &self.back,
        }
    }
}

impl SelectedBuffer {
    fn next(&self) -> Self {
        match self {
            Self::Front => Self::Back,
            Self::Back => Self::Front,
        }
    }
}

#[cfg(all(test, any(feature = "_rt-tokio", feature = "_rt-async-std")))]
mod tests {
    // Cannot use `#[sqlx::test]` because we want to configure the Tokio runtime to use 2 threads
    #[cfg(feature = "_rt-tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_double_buffer_tokio() {
        test_double_buffer().await;
    }

    #[cfg(feature = "_rt-async-std")]
    #[async_std::test]
    async fn test_double_buffer_async_std() {
        test_double_buffer().await;
    }

    async fn test_double_buffer() {
        const CAPACITY: usize = 50;
        const END: usize = 1000;

        let (mut tx, mut rx) = super::channel::<usize>(CAPACITY);

        let reader = crate::rt::spawn(async move {
            for expected in 0usize..=END {
                assert_eq!(rx.recv().await, Some(expected));
            }

            assert_eq!(rx.recv().await, None)
        });

        let writer = crate::rt::spawn(async move {
            for val in 0usize..=END {
                tx.send(val).await.expect("buffer closed prematurely")
            }
        });

        // Our wrapper for `JoinHandle` propagates panics in both cases
        futures_util::future::join(
            reader,
            writer,
        ).await;
    }
}
