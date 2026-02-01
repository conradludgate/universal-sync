//! Proposer configuration

use std::{future::Future, time::Duration};

use rand::{Rng, SeedableRng, rngs::StdRng};

/// Configuration for exponential backoff with jitter
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial backoff duration
    pub initial: Duration,
    /// Maximum backoff duration
    pub max: Duration,
    /// Multiplier for each retry (typically 2.0)
    pub multiplier: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(10),
            max: Duration::from_secs(1),
            multiplier: 2.0,
        }
    }
}

impl BackoffConfig {
    /// Calculate backoff duration for a given retry count with jitter
    #[must_use]
    pub fn duration(&self, retries: u32, rng: &mut impl Rng) -> Duration {
        let base = self.initial.as_secs_f64() * self.multiplier.powi(retries.cast_signed());
        let capped = base.min(self.max.as_secs_f64());
        // Add jitter: 50% to 150% of the base duration
        let jitter_factor = rng.random_range(0.5..1.5);
        Duration::from_secs_f64(capped * jitter_factor)
    }
}

/// Sleep function trait for testing with different runtimes (tokio vs turmoil)
pub trait Sleep: Clone + Send + 'static {
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;
}

/// Tokio-based sleep implementation
#[derive(Clone, Copy, Default)]
pub struct TokioSleep;

impl Sleep for TokioSleep {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Proposer configuration with RNG for jitter
pub struct ProposerConfig<S: Sleep, R: Rng = StdRng> {
    /// Backoff configuration for retries
    pub backoff: BackoffConfig,
    /// Sleep implementation
    pub sleep: S,
    /// RNG for jitter (seeded for deterministic tests)
    pub rng: R,
}

impl<S: Sleep, R: Rng> ProposerConfig<S, R> {
    /// Create a new proposer config with a custom RNG
    pub fn new(backoff: BackoffConfig, sleep: S, rng: R) -> Self {
        Self {
            backoff,
            sleep,
            rng,
        }
    }
}

impl<S: Sleep> ProposerConfig<S, StdRng> {
    /// Create a proposer config with a seeded RNG for deterministic behavior
    #[must_use]
    pub fn with_seed(backoff: BackoffConfig, sleep: S, seed: u64) -> Self {
        Self {
            backoff,
            sleep,
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl Default for ProposerConfig<TokioSleep, StdRng> {
    fn default() -> Self {
        Self {
            backoff: BackoffConfig::default(),
            sleep: TokioSleep,
            rng: StdRng::from_os_rng(),
        }
    }
}
