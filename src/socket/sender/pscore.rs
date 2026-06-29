use crate::{PACKET_WINDOW_LEN, packet::{PacketScore, PacketScoreId}};

pub struct PacketScoreKeeper {
    /// The "unique" wrapping score ID
    id: PacketScoreId, 

    /// The score itself that we can spend on reliable packets
    score: PacketScore,
}

impl PacketScoreKeeper {
    pub fn new() -> Self {

        // The first packet has all the scores, since we're only exploring the connection
        Self {
            id: 0,
            score: PACKET_WINDOW_LEN as PacketScore
        }
    }

    /// Push new score under provided ID. 
    /// 
    /// Doesn't do anything if the ID isn't greater than the current one
    pub fn push_score(&mut self, new_id: PacketScoreId, new_score: PacketScore) {
        assert!((new_score as usize) <= PACKET_WINDOW_LEN);

        // Only overwrite if this ID is more recent.
        // We're using here wrapping trick, so a new wrapped ID of 0, compared to say 254, can be considered new, if the difference is less than 127.
        // That's because our ID are supposed to wrap extremely fast.
        if new_id != self.id && new_id.wrapping_sub(self.id) <= PacketScoreId::MAX/2 {
            self.id = new_id;
            self.score = new_score;
        }
    }

    /// Effective score takes into account round trip time and our delta time, which in turn limits the amount of packets we can actually send.
    /// Why? Because with high RTT, polling frequencies usually stay the same. That means that even with high RTT as 200ms and 60PPS, we can send
    /// up to 12 packets within 200ms interval, which is extremely high.
    /// 
    /// This effectively reduces packet loss and enforces pacing.
    fn effective_score(&self, dt: f64, rtt: f64) -> u8 {
        ((self.score as f64 * dt.abs()/rtt.abs())).clamp(0.0, PACKET_WINDOW_LEN as f64) as u8
    }

    /// Check if we can send any packets as of now or we should wait
    pub fn has_score(&self, packets_in_flight: usize, dt: f64, rtt: f64) -> bool {
        let effective_score = self.effective_score(dt, rtt);

        // We got score if the effective score is greater than 0, or there are less packets in flight than our effective score
        (effective_score > 0) && (packets_in_flight < effective_score as usize)
    }

    pub fn score(&self) -> PacketScore {
        self.score
    }

    /// Consume our score. BUT, only if there is any. Check with [has_score]
    pub fn consume_score(&mut self) {
        assert!(self.score > 0, "Can't consume score, got none");
        
        self.score -= 1;
    }
}