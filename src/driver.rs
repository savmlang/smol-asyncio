use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use parking::Parker;

use crate::reactor::Reactor;

static LAST_TICK: AtomicUsize = AtomicUsize::new(0);

/// Initializes the "async-io" thread.
pub(crate) fn init() {}

/// This is a task queue boun
pub extern "C" fn driverloop() -> bool {
  let tick = Reactor::get().ticker();

  // Get a snapshot
  let last_tick = LAST_TICK.load(Ordering::Acquire);

  // 1. If the ticker moved since last time, we know progress happened elsewhere
  // Update & Progress
  if last_tick != tick {
    LAST_TICK.store(tick, Ordering::Release);
  }

  let reactor_lock = Reactor::get().try_lock();

  if let Some(mut reactor_lock) = reactor_lock {
    #[cfg(feature = "tracing")]
    tracing::trace!("waiting on I/O");

    reactor_lock.react(None).ok();

    LAST_TICK.store(Reactor::get().ticker(), Ordering::Release);
    return true;
  }

  false
}

/// Blocks the current thread on a future, processing I/O events when idle.
///
/// # Examples
///
/// ```
/// use async_io::Timer;
/// use std::time::Duration;
///
/// async_io::block_on(async {
///     // This timer will likely be processed by the current
///     // thread rather than the fallback "async-io" thread.
///     Timer::after(Duration::from_millis(1)).await;
/// });
/// ```
pub fn block_on<T>(future: impl Future<Output = T>) -> T {
  #[cfg(feature = "tracing")]
  let span = tracing::trace_span!("async_io::block_on");
  #[cfg(feature = "tracing")]
  let _enter = span.enter();

  // No Need to store BLOCK_ON_COUNT ANYMORE
  // Increment `BLOCK_ON_COUNT` so that the "async-io" thread becomes less aggressive.
  //   BLOCK_ON_COUNT.fetch_add(1, Ordering::SeqCst);

  // Creates a parker and an associated waker that unparks it.
  fn parker_and_waker() -> (Parker, Waker, Arc<AtomicBool>) {
    // Parker and unparker for notifying the current thread.
    let (p, u) = parking::pair();

    // This boolean is set to `true` when the current thread is blocked on I/O.
    let io_blocked = Arc::new(AtomicBool::new(false));

    // Prepare the waker.
    let waker = BlockOnWaker::create(io_blocked.clone(), u);

    (p, waker, io_blocked)
  }

  thread_local! {
    // Cached parker and waker for efficiency.
    static CACHE: RefCell<(Parker, Waker, Arc<AtomicBool>)> = RefCell::new(parker_and_waker());

    // Indicates that the current thread is polling I/O, but not necessarily blocked on it.
    static IO_POLLING: Cell<bool> = const { Cell::new(false) };
  }

  struct BlockOnWaker {
    io_blocked: Arc<AtomicBool>,
    unparker: parking::Unparker,
  }

  impl BlockOnWaker {
    fn create(io_blocked: Arc<AtomicBool>, unparker: parking::Unparker) -> Waker {
      Waker::from(Arc::new(BlockOnWaker {
        io_blocked,
        unparker,
      }))
    }
  }

  impl std::task::Wake for BlockOnWaker {
    fn wake_by_ref(self: &Arc<Self>) {
      if self.unparker.unpark() {
        // Check if waking from another thread and if currently blocked on I/O.
        if !IO_POLLING.with(Cell::get) && self.io_blocked.load(Ordering::SeqCst) {
          Reactor::get().notify();
        }
      }
    }

    fn wake(self: Arc<Self>) {
      self.wake_by_ref()
    }
  }

  CACHE.with(|cache| {
    // Try grabbing the cached parker and waker.
    let tmp_cached;
    let tmp_fresh;
    let (p, waker, io_blocked) = match cache.try_borrow_mut() {
      Ok(cache) => {
        // Use the cached parker and waker.
        tmp_cached = cache;
        &*tmp_cached
      }
      Err(_) => {
        // Looks like this is a recursive `block_on()` call.
        // Create a fresh parker and waker.
        tmp_fresh = parker_and_waker();
        &tmp_fresh
      }
    };

    let mut future = pin!(future);

    let cx = &mut Context::from_waker(waker);

    loop {
      // Poll the future.
      if let Poll::Ready(t) = future.as_mut().poll(cx) {
        // Ensure the cached parker is reset to the unnotified state for future block_on calls,
        // in case this future called wake and then immediately returned Poll::Ready.
        p.park_timeout(Duration::from_secs(0));
        #[cfg(feature = "tracing")]
        tracing::trace!("completed");
        return t;
      }

      // Check if a notification was received.
      if p.park_timeout(Duration::from_secs(0)) {
        #[cfg(feature = "tracing")]
        tracing::trace!("notified");

        // Try grabbing a lock on the reactor to process I/O events.
        if let Some(mut reactor_lock) = Reactor::get().try_lock() {
          // First let wakers know this parker is processing I/O events.
          IO_POLLING.with(|io| io.set(true));
          let _guard = CallOnDrop(|| {
            IO_POLLING.with(|io| io.set(false));
          });

          // Process available I/O events.
          reactor_lock.react(Some(Duration::from_secs(0))).ok();
        }
        continue;
      }

      // Try grabbing a lock on the reactor to wait on I/O.
      if let Some(mut reactor_lock) = Reactor::get().try_lock() {
        // Record the instant at which the lock was grabbed.
        let start = Instant::now();

        loop {
          // First let wakers know this parker is blocked on I/O.
          IO_POLLING.with(|io| io.set(true));
          io_blocked.store(true, Ordering::SeqCst);
          let _guard = CallOnDrop(|| {
            IO_POLLING.with(|io| io.set(false));
            io_blocked.store(false, Ordering::SeqCst);
          });

          // Check if a notification has been received before `io_blocked` was updated
          // because in that case the reactor won't receive a wakeup.
          if p.park_timeout(Duration::from_secs(0)) {
            #[cfg(feature = "tracing")]
            tracing::trace!("notified");
            break;
          }

          // Wait for I/O events.
          #[cfg(feature = "tracing")]
          tracing::trace!("waiting on I/O");
          reactor_lock.react(None).ok();

          // Check if a notification has been received.
          if p.park_timeout(Duration::from_secs(0)) {
            #[cfg(feature = "tracing")]
            tracing::trace!("notified");
            break;
          }

          // Check if this thread been handling I/O events for a long time.
          if start.elapsed() > Duration::from_micros(500) {
            #[cfg(feature = "tracing")]
            tracing::trace!("stops hogging the reactor");

            // This thread is clearly processing I/O events for some other threads
            // because it didn't get a notification yet. It's best to stop hogging the
            // reactor and give other threads a chance to process I/O events for
            // themselves.
            drop(reactor_lock);

            // Unpark is no longer requried (or, in our control)
            //
            // Unpark the "async-io" thread in case no other thread is ready to start
            // processing I/O events. This way we prevent a potential latency spike.
            // unparker().unpark();

            // Wait for a notification.
            p.park();
            break;
          }
        }
      } else {
        // Wait for an actual notification.
        #[cfg(feature = "tracing")]
        tracing::trace!("sleep until notification");
        p.park();
      }
    }
  })
}

/// Runs a closure when dropped.
struct CallOnDrop<F: Fn()>(F);

impl<F: Fn()> Drop for CallOnDrop<F> {
  fn drop(&mut self) {
    (self.0)();
  }
}
