# named-retry

This is a simple, `impl Copy` utility for retrying fallible asynchronous operations, with helpful log messages through `tracing`.

```rust
use std::time::Duration;
use named_retry::Retry;

let retry = Retry::new("test")
    .attempts(5)
    .base_delay(Duration::from_secs(1))
    .delay_factor(2.0);

let result = retry.run(|| async { Ok::<_, ()>("done!") }).await;
assert_eq!(result, Ok("done!"));
```
