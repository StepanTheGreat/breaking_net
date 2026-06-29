use std::{io, net::SocketAddr, time::Duration};

use rand::{Rng, SeedableRng, rngs::SmallRng};

use crate::socket::{SocketBackend, SocketUDP};

fn is_valid_chance(chance: f32) -> bool {
    (0.0..=1.0).contains(&chance)
}

/// Generate a random chance with a specific chance (from 0 to 1)
fn rand_chance(rng: &mut SmallRng, chance: f32) -> bool {
    assert!(is_valid_chance(chance));

    rng.random_range(0.0..=1.0) <= chance
}

/// Unreliability settings used for the virtual socket
#[derive(Clone, Copy, Debug)]
pub struct VirtSettings {
    packet_loss_rate: f32,
    latency: Duration,
    jitter: Duration,
    corruption_rate: f32,
    dublicate_rate: f32,
}

impl Default for VirtSettings {
    fn default() -> Self {
        Self {
            packet_loss_rate: 0.0,
            latency: Duration::ZERO,
            jitter: Duration::ZERO,
            corruption_rate: 0.0,
            dublicate_rate: 0.0,
        }
    }
}

impl VirtSettings {
    /// Set corruption rate (simply shuffles packet's data) between 0 and 1
    pub fn set_corruption_rate(&mut self, new_rate: f32) -> &mut Self {
        assert!(is_valid_chance(new_rate));

        self.corruption_rate = new_rate;

        self
    }

    /// Set packet loss rate (should a packet be lost) between 0 and 1
    pub fn set_packet_loss_rate(&mut self, new_rate: f32) -> &mut Self {
        assert!(is_valid_chance(new_rate));

        self.packet_loss_rate = new_rate;

        self
    }

    /// Should a packet be dublicated or not (between 0 and 1)
    pub fn set_dublicate_rate(&mut self, new_rate: f32) -> &mut Self {
        assert!(is_valid_chance(new_rate));

        self.dublicate_rate = new_rate;

        self
    }

    /// How much should each packet take to arrive
    pub fn set_latency(&mut self, val: Duration) -> &mut Self {
        self.latency = val;

        self
    }

    /// What's the average jitter added to the arrival latency
    pub fn set_jitter(&mut self, val: Duration) -> &mut Self {
        self.jitter = val;

        self
    }
}

/// A UDP socket with added simulation features (latency, packet loss and so on).
///
/// Neccessary for simulations and tests
pub struct VirtSocketUDP {
    socket: SocketUDP,
    settings: VirtSettings,
    rng: SmallRng,

    time: Duration,
    packets: Vec<(Box<[u8]>, SocketAddr, Duration)>,

    /// A temporary buffer for packet removals
    remove_buff: Vec<usize>,
}

impl VirtSocketUDP {
    pub fn new(socket: SocketUDP, settings: VirtSettings) -> Self {
        Self {
            socket,
            settings,
            rng: SmallRng::from_os_rng(),

            time: Duration::ZERO,
            packets: Vec::new(),

            remove_buff: Vec::new(),
        }
    }

    pub fn settings_mut(&mut self) -> &mut VirtSettings {
        &mut self.settings
    }
}

impl SocketBackend for VirtSocketUDP {
    fn addr(&self) -> SocketAddr {
        self.socket.addr()
    }

    fn has_messages(&self) -> bool {
        self.socket.has_messages()
    }

    fn poll(&mut self, dt: Duration) {
        self.time += dt;

        // Check if a packet can be sent
        for (ind, (_, _, time)) in self.packets.iter().enumerate() {
            if self.time >= *time {
                self.remove_buff.push(ind);
            }
        }

        // If so, remove it and send
        for ind in self.remove_buff.iter().copied().rev() {
            let (packet, addr, _) = self.packets.remove(ind);
            let _ = self.socket.send_to(&packet, addr);
        }

        self.remove_buff.clear();
    }

    fn recv_from(&mut self) -> Option<(&[u8], SocketAddr)> {
        self.socket.recv_from()
    }

    fn send_to(&mut self, data: &[u8], to: SocketAddr) -> io::Result<()> {
        let mut data = Box::from_iter(data.iter().copied());

        // Should we lose this packet?
        if rand_chance(&mut self.rng, self.settings.packet_loss_rate) {
            return Ok(());
        }

        // We'll simply inverse data if it's supposed to get "corrupted"
        if rand_chance(&mut self.rng, self.settings.corruption_rate) {
            data.reverse();
        }

        // We may send the same packet twice
        let times = if rand_chance(&mut self.rng, self.settings.dublicate_rate) {
            2
        } else {
            1
        };

        for _ in 0..times {
            // Compute packet arrival time
            let latency = self.settings.latency;

            // Compute the jitter. It can absolutely be negative, which is why we might actually subtract it from our time
            let jitter = self
                .settings
                .jitter
                .mul_f32(self.rng.random_range(0.0..=1.0));

            let latency = if rand_chance(&mut self.rng, 0.5) {
                latency.saturating_add(jitter)
            } else {
                latency.saturating_sub(jitter)
            };

            self.packets.push((data.clone(), to, self.time+latency));
        }

        // This is done to avoid waiting for another cycle to actually send packets via sockets
        self.poll(Duration::ZERO);

        Ok(())
    }
}
