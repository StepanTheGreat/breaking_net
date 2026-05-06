use std::time::Duration;

use crate::utils::Circular;

// We'll keep it at 500ms
const RTT_RECORD_FREQ: Duration = Duration::from_millis(500);

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
    pub fn update(&mut self, dt: Duration) {
        self.next_rtt_record = self.next_rtt_record.saturating_sub(dt);

        if self.next_rtt_record.is_zero() {
            self.next_rtt_record = RTT_RECORD_FREQ;
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

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use crate::assert_eq_eps;

use super::*;

    #[test]
    fn test_measurements() {
        const RTT: Duration = Duration::from_millis(50);
        const DEV: Duration = Duration::from_millis(10);
        const DT: Duration = RTT_RECORD_FREQ;
        const EPS: f64 = 0.02;

        let mut stats = RTTMeasurements::new(
            RTT, 
            DEV, 
            0.8, 
            20
        );

        // By default there are no samples
        assert_eq_eps!(stats.median(), RTT.as_secs_f64(), EPS);
        assert_eq_eps!(stats.rtt(), RTT.as_secs_f64(), EPS);
        assert_eq_eps!(stats.deviation(), DEV.as_secs_f64(), EPS);

        dbg!(stats.median());

        // Push a few really lucky packets
        for _ in 0..2 {
            stats.push(Duration::from_millis(20));
            stats.update(DT);
            dbg!(stats.median());
        }

        // Our RTT should go down a little bit as a reaction
        assert!(stats.rtt() < RTT.as_secs_f64());

        // Let's push a few more packets (and record them)
        for _ in 0..10 {
            stats.push(RTT);
            stats.update(DT);
            dbg!(stats.median());
        }
        
        // No changes in median or deviation
        assert_eq_eps!(stats.rtt(), RTT.as_secs_f64(), EPS);
        assert_eq_eps!(stats.deviation(), DEV.as_secs_f64(), EPS);
        dbg!(stats.median());
        assert_eq_eps!(stats.median(), RTT.as_secs_f64(), EPS);


        // Now we'll push and record two terrible latencies
        for _ in 0..2 {
            stats.push(Duration::from_millis(500));
            stats.update(DT);
        }

        // Our RTT must be drastically different, BUT, our median should stay the same, since according to history network's average
        // was constantly 50ms. 
        assert_eq_eps!(stats.median(), RTT.as_secs_f64(), EPS);
        assert!(stats.rtt() > stats.median());
        assert!(stats.deviation() > DEV.as_secs_f64());

    }
}