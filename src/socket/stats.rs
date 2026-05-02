use std::time::Duration;

use crate::utils::Circular;

/// Exponential Moving Average structure for convenient exponential smoothing of continous samples.
pub struct Ema {
    data: f64,
    alpha: f64,
}

impl Ema {
    pub fn new(init: f64, alpha: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&alpha),
            "Alpha must be in range between 0 and 1"
        );

        Self { data: init, alpha }
    }

    /// Push a new sample
    pub fn push(&mut self, sample: f64) {
        self.data = (1.0 - self.alpha) * self.data + self.alpha * sample;
    }

    /// Get the current smoothed average
    pub fn get(&self) -> f64 {
        self.data
    }
}

/// Statistics about the connection like its RTT and its deviation
pub struct RTTMeasurements {
    rtt: Ema,

    rtt_dev: Ema,

    /// History of the most relevant RTT samples (updated quite rarely, maybe once a second). Important for finding the median RTT.
    rtt_hist: Circular<f64>,

    /// A simply acts as a temporary buffer for sorting RTT history samples and finding the median
    rtt_hist_buff: Vec<f64>,

    /// When to record next average RTT sample
    next_rtt_record: Duration,
}

impl RTTMeasurements {
    pub fn new(init_rtt: Duration, init_dev: Duration, alpha: f64, history_len: usize) -> Self {
        Self {
            rtt: Ema::new(init_rtt.as_secs_f64(), alpha),
            rtt_dev: Ema::new(init_dev.as_secs_f64(), alpha),
            rtt_hist: Circular::new(history_len),
            rtt_hist_buff: Vec::with_capacity(history_len),
            next_rtt_record: Duration::ZERO,
        }
    }

    /// Record the current RTT value. This should be called infrequently, since this clears internal buffer and resorts all samples
    fn record_rtt(&mut self) {
        // Push a new sample
        self.rtt_hist.push(self.rtt.get());

        // Re-fill our history buffer
        self.rtt_hist_buff.clear();
        self.rtt_hist_buff.extend(self.rtt_hist.inner());

        // Sort our samples
        self.rtt_hist_buff.sort_by(|a, b| a.total_cmp(b));
    }

    /// RTT measurements must be updated to keep recording average RTT history and computing the median
    pub fn update(&mut self, time: Duration) {
        if time > self.next_rtt_record {
            self.next_rtt_record = time + Duration::from_millis(500);
            self.record_rtt();
        }
    }

    /// Push a new delta into this RTT tracker
    pub fn push(&mut self, dt: Duration) {
        let dt = dt.as_secs_f64();

        self.rtt.push(dt);

        // Our deviation is based on MAD https://en.wikipedia.org/wiki/Median_absolute_deviation
        // Which doesn't gives as much attention to outliers compared to standard deviation
        self.rtt_dev.push((dt - self.rtt.get()).abs());
    }

    /// Get average RTT
    pub fn rtt(&self) -> f64 {
        self.rtt.get()
    }

    /// Get median average RTT deviation
    pub fn deviation(&self) -> f64 {
        self.rtt_dev.get()
    }

    /// Median RTT. Compared to our average RTT, this one is not smoothed out and reflects more the *speed* of the network over longer period of time.
    /// It's particularly useful for knowing the **base** RTT.
    pub fn median(&self) -> f64 {
        let len = self.rtt_hist_buff.len();

        if len == 0 {
            self.rtt.get()
        } else if !len.is_multiple_of(2) {
            // If our buffer has an odd number of elements - just return the middle one
            self.rtt_hist_buff[len / 2]
        } else {
            // In case of event number of elements, we're going to return an average of two middle samples
            let a = self.rtt_hist_buff[(len - 1) / 2];
            let b = self.rtt_hist_buff[len / 2];

            (a + b) / 2.0
        }
    }
}
