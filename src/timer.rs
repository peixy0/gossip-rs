use std::future::Future;
use tokio::time::Duration;
use tracing::*;

pub trait Timer {}

pub trait TimerService {
    type Timer: Send;
    fn create(
        &self,
        duration: Duration,
        f: impl Future<Output = ()> + Send + 'static,
    ) -> Self::Timer;
}

pub struct DefaultTimer(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for DefaultTimer {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

impl Timer for DefaultTimer {}

#[derive(Clone)]
pub struct DefaultTimerService;

impl TimerService for DefaultTimerService {
    type Timer = DefaultTimer;

    fn create(
        &self,
        duration: Duration,
        f: impl Future<Output = ()> + Send + 'static,
    ) -> Self::Timer {
        let marker = uuid::Uuid::new_v4();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            trace!("Timer {} started for {}ms", marker, duration.as_millis());
            tokio::select! {
                _ = tokio::time::sleep(duration) => {
                    trace!("Timer {} triggered", marker);
                    f.await;
                }
                _ = rx => {
                    trace!("Timer {} cancelled", marker);
                }
            }
        });
        DefaultTimer(Some(tx))
    }
}
