use std::collections::HashMap;

use crate::packet::PacketSeqId;

const RTT_SMOOTH_FACTOR: f32 = 0.4;
const RTT_MAX_TIME: f32 = 1.0;

const INIT_RTT: f32 = 0.0;

pub struct StatisticsManager {
    rtt_timers: HashMap<PacketSeqId, f32>,

    /// The approximate RTT
    rtt: f32,
}

impl StatisticsManager {
    pub fn new() -> Self {
        Self {
            rtt_timers: HashMap::new(),
            rtt: INIT_RTT,
        }
    }

    /// Update RTT timers and when some of them are maxed out - remove them
    fn update_rtt_timers(&mut self, dt: f32) {
        self.rtt_timers.retain(|_, timer| {
            *timer = (*timer + dt).min(1.0);

            // Only keep those that didn't timed out
            *timer != RTT_MAX_TIME
        });
    }
}
