use tokio::time::Duration;
use tracing::*;

pub struct Timer(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

impl Timer {
    pub fn new(duration: Duration, f: impl Future<Output = ()> + Send + 'static) -> Self {
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
        Self(Some(tx))
    }
}
