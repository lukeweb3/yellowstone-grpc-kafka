pub mod config;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "metrics")]
pub mod metrics;

pub mod version;
pub mod generated;

use {
    futures::future::{BoxFuture, FutureExt},
    std::io::{self, IsTerminal},
    tokio::signal::unix::{signal, SignalKind},
    tracing_subscriber::{
        filter::{EnvFilter, LevelFilter},
        layer::SubscriberExt,
        util::SubscriberInitExt,
    },
};

pub fn setup_tracing() -> anyhow::Result<()> {
    let is_atty = io::stdout().is_terminal() && io::stderr().is_terminal();
    let io_layer = tracing_subscriber::fmt::layer().with_ansi(is_atty);
    let level_layer = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(io_layer)
        .with(level_layer)
        .try_init()?;
    Ok(())
}

pub fn create_shutdown() -> anyhow::Result<BoxFuture<'static, ()>> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = sigint.recv() => {},
            _ = sigterm.recv() => {}
        };
    }
    .boxed())
}
