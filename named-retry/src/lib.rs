//! Utilities for retrying falliable, asynchronous operations.

use std::future::Future;
use std::time::Duration;

use tokio::time;
use tracing::warn;

/// Calls a fallible async function multiple times, with a given timeout.
///
/// If a `base_delay` is provided, the function is given an exponentially
/// increasing delay on each run, up until the maximum number of attempts.
///
/// Returns the first successful result if any, or the last error.
#[derive(Copy, Clone, Debug)]
pub struct Retry {
    /// Name of the operation being retried.
    pub name: &'static str,

    /// The number of attempts to make.
    pub attempts: u32,

    /// The base delay after the first attempt, if provided.
    pub base_delay: Duration,

    /// Exponential factor to increase the delay by on each attempt.
    pub delay_factor: f64,
}

impl Retry {
    /// Construct a new [`Retry`] object with default parameters.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            attempts: 3,
            base_delay: Duration::ZERO,
            delay_factor: 1.0,
        }
    }

    /// Set the number of attempts to make.
    pub const fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    /// Set the base delay.
    pub const fn base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }

    /// Set the exponential factor increasing delay.
    pub const fn delay_factor(mut self, delay_factor: f64) -> Self {
        self.delay_factor = delay_factor;
        self
    }

    /// Run a falliable asynchronous function using this retry configuration.
    ///
    /// Panics if the number of attempts is set to `0`, or the base delay is
    /// incorrectly set to a negative duration.
    pub async fn run<T, E, Fut>(self, mut func: impl FnMut() -> Fut) -> Result<T, E>
    where
        Fut: Future<Output = Result<T, E>>,
    {
        assert!(self.attempts > 0, "attempts must be greater than 0");
        assert!(
            self.base_delay >= Duration::ZERO && self.delay_factor >= 0.0,
            "retry delay cannot be negative"
        );
        let mut delay = self.base_delay;
        for i in 0..self.attempts {
            match func().await {
                Ok(value) => return Ok(value),
                Err(err) if i == self.attempts - 1 => return Err(err),
                _ => {
                    warn!(name = %self.name, attempt = i, ?delay, "failed invoking function, retrying");
                    time::sleep(delay).await;
                    delay = delay.mul_f64(self.delay_factor);
                }
            }
        }
        unreachable!();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::Retry;

    #[tokio::test]
    #[should_panic]
    async fn zero_retry_attempts() {
        let _ = Retry::new("test")
            .attempts(0)
            .run(|| async { Ok::<_, std::io::Error>(()) })
            .await;
    }

    #[tokio::test]
    async fn successful_retry() {
        let mut count = 0;
        let task = Retry::new("test").run(|| {
            count += 1;
            async { Ok::<_, std::io::Error>(()) }
        });
        let result = task.await;
        assert_eq!(count, 1);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn failed_retry() {
        let mut count = 0;
        let retry = Retry::new("test");
        let task = retry.run(|| {
            count += 1;
            async { Err::<(), ()>(()) }
        });
        let result = task.await;
        assert_eq!(count, retry.attempts);
        assert!(result.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_retry() {
        let start = Instant::now();

        let mut count = 0;
        // Will retry at 0s, 1s, 3s, 7s, 15s
        let task = Retry::new("test")
            .attempts(5)
            .base_delay(Duration::from_secs(1))
            .delay_factor(2.0)
            .run(|| {
                count += 1;
                async {
                    println!("elapsed = {:?}", start.elapsed());
                    if start.elapsed() < Duration::from_secs(5) {
                        Err::<(), ()>(())
                    } else {
                        Ok(())
                    }
                }
            });
        let result = task.await;
        assert_eq!(count, 4);
        assert!(result.is_ok());
    }
}
