use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

#[cfg(feature = "_rt-async-std")]
pub mod rt_async_std;

#[cfg(feature = "_rt-tokio")]
pub mod rt_tokio;

//#[cfg(target_arch = "wasm32")]
pub mod rt_wasip3;

#[derive(Debug, thiserror::Error)]
#[error("operation timed out")]
pub struct TimeoutError(());

pub enum JoinHandle<T> {
    #[cfg(feature = "_rt-async-std")]
    AsyncStd(async_std::task::JoinHandle<T>),
    #[cfg(feature = "_rt-tokio")]
    Tokio(tokio::task::JoinHandle<T>),
    // `PhantomData<T>` requires `T: Unpin`
    _Phantom(PhantomData<fn() -> T>),
}

pub async fn timeout<F: Future>(duration: Duration, f: F) -> Result<F::Output, TimeoutError> {
    #[cfg(target_arch = "wasm32")]
    {
        let timeout = crate::rt::rt_wasip3::spawn(wasi::clocks::monotonic_clock::wait_for(
            duration.as_nanos().try_into().unwrap_or(u64::MAX),
        ));
        let mut timeout = core::pin::pin!(timeout);
        let mut f = core::pin::pin!(f);
        core::future::poll_fn(|cx| {
            match timeout.as_mut().poll(cx) {
                Poll::Ready(Some(())) => {
                    Poll::Ready(Err(TimeoutError(())))
                }
                Poll::Ready(None) => {
                    Poll::Ready(Err(TimeoutError(())))
                }
                Poll::Pending => {
                    f.as_mut().poll(cx).map(Ok)
                }
            }
        })
        .await
    }

    #[cfg(all(feature = "_rt-tokio", not(target_arch = "wasm32")))]
    if rt_tokio::available() {
        return tokio::time::timeout(duration, f)
            .await
            .map_err(|_| TimeoutError(()));
    }

    #[cfg(feature = "_rt-async-std")]
    {
        async_std::future::timeout(duration, f)
            .await
            .map_err(|_| TimeoutError(()))
    }

    #[cfg(not(any(feature = "_rt-async-std", target_arch = "wasm32")))]
    missing_rt((duration, f))
}

pub async fn sleep(duration: Duration) {
    #[cfg(target_arch = "wasm32")]
    {
        return crate::rt::rt_wasip3::spawn(wasi::clocks::monotonic_clock::wait_for(
            duration.as_nanos().try_into().unwrap_or(u64::MAX),
        ))
        .await
        .unwrap();
    }

    #[cfg(feature = "_rt-tokio")]
    if rt_tokio::available() {
        return tokio::time::sleep(duration).await;
    }

    #[cfg(feature = "_rt-async-std")]
    {
        async_std::task::sleep(duration).await
    }

    #[cfg(not(any(feature = "_rt-async-std", target_arch = "wasm32")))]
    missing_rt(duration)
}

#[cfg(not(target_arch = "wasm32"))]
#[track_caller]
pub fn spawn<F>(fut: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    #[cfg(feature = "_rt-tokio")]
    if let Ok(..) = tokio::runtime::Handle::try_current() {
        return JoinHandle::Tokio(tokio::task::spawn_local(fut));
    }

    #[cfg(feature = "_rt-async-std")]
    {
        JoinHandle::AsyncStd(async_std::task::spawn(fut))
    }

    #[cfg(not(any(feature = "_rt-async-std", target_arch = "wasm32")))]
    missing_rt(fut)
}

#[cfg(target_arch = "wasm32")]
#[track_caller]
pub fn spawn<F>(fut: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    JoinHandle::Tokio(tokio::task::spawn_local(fut))
}

#[cfg(not(target_arch = "wasm32"))]
#[track_caller]
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    #[cfg(feature = "_rt-tokio")]
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return JoinHandle::Tokio(handle.spawn_blocking(f));
    }

    #[cfg(feature = "_rt-async-std")]
    {
        JoinHandle::AsyncStd(async_std::task::spawn_blocking(f))
    }

    #[cfg(not(feature = "_rt-async-std"))]
    missing_rt(f)
}

pub async fn yield_now() {
    #[cfg(feature = "_rt-tokio")]
    if rt_tokio::available() {
        return tokio::task::yield_now().await;
    }

    #[cfg(feature = "_rt-async-std")]
    {
        async_std::task::yield_now().await;
    }

    #[cfg(not(feature = "_rt-async-std"))]
    missing_rt(())
}

#[track_caller]
pub fn test_block_on<F: Future>(f: F) -> F::Output {
    #[cfg(any(feature = "_rt-tokio", target_arch = "wasm32"))]
    {
        return tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to start Tokio runtime")
            .block_on(f);
    }

    #[cfg(all(
        feature = "_rt-async-std",
        not(any(feature = "_rt-tokio", target_arch = "wasm32"))
    ))]
    {
        async_std::task::block_on(f)
    }

    #[cfg(not(any(
        feature = "_rt-async-std",
        feature = "_rt-tokio",
        target_arch = "wasm32"
    )))]
    {
        missing_rt(f)
    }
}

#[track_caller]
pub fn missing_rt<T>(_unused: T) -> ! {
    if cfg!(feature = "_rt-tokio") {
        panic!("this functionality requires a Tokio context")
    }

    panic!("either the `runtime-async-std` or `runtime-tokio` feature must be enabled")
}

impl<T: Send + 'static> Future for JoinHandle<T> {
    type Output = T;

    #[track_caller]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut *self {
            #[cfg(feature = "_rt-async-std")]
            Self::AsyncStd(handle) => Pin::new(handle).poll(cx),
            #[cfg(any(feature = "_rt-tokio", target_arch = "wasm32"))]
            Self::Tokio(handle) => Pin::new(handle)
                .poll(cx)
                .map(|res| res.expect("spawned task panicked")),
            Self::_Phantom(_) => {
                let _ = cx;
                unreachable!("runtime should have been checked on spawn")
            }
        }
    }
}
