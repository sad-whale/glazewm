use std::{
  sync::{mpsc, Arc, Mutex},
  time::Duration,
};

use crate::DispatchFn;

struct PendingDispatch<F>(Arc<Mutex<Option<F>>>);

impl<F> Drop for PendingDispatch<F> {
  fn drop(&mut self) {
    // Cancel queued work; wait for running work before its borrows expire.
    let callback = self
      .0
      .lock()
      .unwrap_or_else(std::sync::PoisonError::into_inner)
      .take();
    drop(callback);
  }
}

pub(crate) fn dispatch_sync<F, R>(
  dispatch_fn: F,
  schedule: impl FnOnce(Box<DispatchFn>) -> crate::Result<()>,
  timeout: Duration,
) -> crate::Result<R>
where
  F: FnOnce() -> R + Send,
  R: Send,
{
  let pending = PendingDispatch(Arc::new(Mutex::new(Some(dispatch_fn))));
  let callback = pending.0.clone();
  let (result_tx, result_rx) = mpsc::channel();
  let callback: Box<dyn FnOnce() + Send + '_> = Box::new(move || {
    let mut callback = callback
      .lock()
      .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(dispatch_fn) = callback.take() {
      let _ = result_tx.send(dispatch_fn());
    }
  });

  // SAFETY: The guard removes queued closures or waits for running ones on
  // every exit path, including timeout, scheduling failure and unwinding.
  let callback = unsafe {
    std::mem::transmute::<Box<dyn FnOnce() + Send + '_>, Box<DispatchFn>>(
      callback,
    )
  };
  schedule(callback)?;
  result_rx
    .recv_timeout(timeout)
    .map_err(crate::Error::ChannelRecv)
}

#[cfg(test)]
mod tests {
  use std::sync::atomic::{AtomicBool, Ordering};

  use super::*;

  #[test]
  fn returns_borrowed_results() {
    let value = String::from("borrowed");
    let result = dispatch_sync(
      || value.as_str(),
      |callback| {
        callback();
        Ok(())
      },
      Duration::ZERO,
    );
    assert_eq!(result.unwrap(), "borrowed");
  }

  #[test]
  fn timed_out_work_never_runs_later() {
    let mut called = false;
    let mut queued = None;
    let result = dispatch_sync(
      || called = true,
      |callback| {
        queued = Some(callback);
        Ok(())
      },
      Duration::ZERO,
    );
    assert!(matches!(
      result,
      Err(crate::Error::ChannelRecv(mpsc::RecvTimeoutError::Timeout))
    ));
    queued.take().unwrap()();
    assert!(!called);
  }

  #[test]
  fn scheduling_failure_cancels_retained_work() {
    let mut called = false;
    let mut queued = None;
    let result = dispatch_sync(
      || called = true,
      |callback| {
        queued = Some(callback);
        Err(crate::Error::ChannelSend)
      },
      Duration::ZERO,
    );
    assert!(matches!(result, Err(crate::Error::ChannelSend)));
    queued.take().unwrap()();
    assert!(!called);
  }

  #[test]
  fn scheduling_panic_cancels_retained_work() {
    let mut called = false;
    let mut queued = None;
    let result =
      std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        dispatch_sync(
          || called = true,
          |callback| {
            queued = Some(callback);
            panic!("scheduling failed");
          },
          Duration::ZERO,
        )
      }));
    assert!(result.is_err());
    queued.take().unwrap()();
    assert!(!called);
  }

  #[test]
  fn timeout_waits_for_running_work_to_release_borrows() {
    let finished = Arc::new(AtomicBool::new(false));
    let finished_clone = finished.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let _ = dispatch_sync(
      move || {
        started_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        finished_clone.store(true, Ordering::SeqCst);
      },
      |callback| {
        std::thread::spawn(callback);
        started_rx.recv().unwrap();
        Ok(())
      },
      Duration::ZERO,
    );
    assert!(finished.load(Ordering::SeqCst));
  }
}
