use crate::packet::PacketSeqId;

type BitPage = u64;

/// The amount of bits in our bit page
const PAGE_BITS: usize = BitPage::BITS as _;

/// A super minimal bitset implementation which allows setting bits at arbitrary positions and shifting the entire structure to the right
#[derive(Clone)]
pub struct BitSet {
    pages: Box<[BitPage]>,
    bit_len: usize,
}

impl BitSet {
    pub fn new(bit_len: usize) -> Self {
        assert!(bit_len > 0, "A bit array can't have zero bits");

        // Calculate the amount of pages needed
        let pages_len = bit_len.div_ceil(PAGE_BITS);

        let pages = vec![0; pages_len].into_boxed_slice();

        Self { bit_len, pages }
    }

    /// Set a bit at the provided location
    pub fn set(&mut self, bind: usize, to: bool) {
        let value = self.get(bind);

        let ind = bind / PAGE_BITS;
        let offset = bind % PAGE_BITS;

        if value != to {
            self.pages[ind] ^= 1 << ((PAGE_BITS - 1) - offset);
        }
    }

    /// Directly put an entire page at a page position
    pub fn put(&mut self, index: usize, page: BitPage) {
        self.pages[index] = page;
    }

    pub fn read(&self, index: usize) -> BitPage {
        self.pages[index]
    }

    /// Get the value of the provided bit (indexed by bit index)
    pub fn get(&self, index: usize) -> bool {
        assert!(index < self.bit_len());

        let offset = index % PAGE_BITS;

        (self.pages[index / PAGE_BITS] & (1 << ((PAGE_BITS - 1) - offset))) > 0
    }

    /// The amount of pages this bitset contains (a single page containing multiple bits)
    pub fn len(&self) -> usize {
        self.bit_len() / PAGE_BITS
    }

    /// The length of this bitset in bits
    pub fn bit_len(&self) -> usize {
        self.bit_len
    }

    /// Shift this structure to the right
    pub fn shr(&mut self, mut by: usize) {
        let page_len = self.len();

        // When we're shifting pages, we can shift by entire integers at once. This is a pretty slow operation,
        if by >= PAGE_BITS {
            // By how many pages to shift
            let shift_pages = by / PAGE_BITS;

            // Decrement by an entire page
            by -= shift_pages * PAGE_BITS;

            // For each page index, starting from 1
            for ind in (0..page_len).rev() {
                let new_ind = ind + shift_pages;

                if new_ind >= page_len {
                    continue;
                }

                // Swap it with the page to its left
                self.pages[new_ind] = self.pages[ind];
            }

            // The first pages will simply turn into zeros
            for ind in 0..shift_pages {
                self.pages[ind] = 0;
            }
        }

        // For each set (iterating from the right)
        for i in (0..page_len).rev() {
            // If it's the last page - simply shift it
            if i == page_len - 1 {
                self.pages[i] >>= by;
            } else {
                // In any other case we're going to shift our current page onto the right one
                self.pages[i + 1] |= self.pages[i] << (PAGE_BITS - by);

                // And now shift our own page
                self.pages[i] >>= by;
            }
        }
    }

    pub fn as_ref(&self) -> &[BitPage] {
        &self.pages
    }
}

/// The sliding window helps tracking unacknowledged packets. 
/// It has a base and a bitset, where each received packet coming after the base is marked with 1.
/// 
/// The window automatically slides whenever the window's lowest packet (the base) is received. 
pub struct SlidingAckWindow {
    /// The position of the window (the oldest packet) to not get acknowledged
    window_pos: PacketSeqId,

    /// The frame storage itself (has a constant size)
    frames: BitSet,
}

/// The mark of a packet, which describes its status in the packet window
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketMark {
    /// The packet is new (out of window bounds)
    OutOfReach,

    /// The packet is within the bounds (marked)
    Marked,

    /// The packet is within the bounds (not marked)
    NonMarked,

    /// THe packet is old (out of bounds)
    Old,
}

impl SlidingAckWindow {
    /// Create a new sliding window with the provided amount of packets.
    pub fn new(packet_len: usize) -> Self {
        let frames = BitSet::new(packet_len);

        Self {
            window_pos: 0,
            frames,
        }
    }

    /// Mark this packet
    pub fn mark(&mut self, packet: PacketSeqId) {
        // The packet is older than our window, meaning it's a dublicate
        if packet < self.window_pos {
            return;
        }

        // Let's compute the packet's index
        let ind = (packet - self.window_pos) as usize;

        if ind >= self.frames.bit_len {
            // We can't possibly mark it
            return;
        }

        // Mark it
        self.frames.set((self.frames.bit_len() - 1) - ind, true);

        // Finally, while our topmost bit is 1 (meaning our window can actually slide)
        while self.frames.get(self.frames.bit_len() - 1) {
            // We're going to shift our bits to the right by 1
            self.frames.shr(1);

            // And increment our position
            self.window_pos += 1;
        }
    }

    /// Get the mark status for the provided packet
    pub fn get_marked(&self, packet: PacketSeqId) -> PacketMark {
        if packet < self.window_pos {
            return PacketMark::Old;
        }

        let ind = (packet - self.window_pos) as usize;

        if ind >= self.frames.bit_len {
            // This packet can't be processed for now
            return PacketMark::OutOfReach;
        }

        // In any other case we're going to check the window
        match self.frames.get((self.frames.bit_len() - 1) - ind) {
            true => PacketMark::Marked,
            false => PacketMark::NonMarked,
        }
    }

    /// Get the lowest window position sequence
    pub fn window_position(&self) -> PacketSeqId {
        self.window_pos
    }

    /// Check if this packet is old (no longer within the window bounds)
    pub fn is_old(&self, packet: PacketSeqId) -> bool {
        self.get_marked(packet) == PacketMark::Old
    }

    pub fn is_out_of_reach(&self, packet: PacketSeqId) -> bool {
        self.get_marked(packet) == PacketMark::OutOfReach
    }

    /// Check if this packet is within window's bounds
    pub fn within_bounds(&self, packet: PacketSeqId) -> bool {
        let m = self.get_marked(packet);

        m == PacketMark::Marked || m == PacketMark::NonMarked
    }

    pub fn is_marked(&self, packet: PacketSeqId) -> bool {
        self.get_marked(packet) == PacketMark::Marked
    }
}

#[cfg(test)]
mod tests {
    use crate::window::{BitPage, BitSet, PAGE_BITS, SlidingAckWindow};

    #[test]
    fn test_bitset_set() {
        let mut bitset = BitSet::new(256);

        for ind in 0..16 {
            bitset.set(ind, true);
            assert_eq!(bitset.get(ind), true);
        }

        bitset.shr(2);

        assert_eq!(bitset.get(0), false);
        assert_eq!(bitset.get(1), false);

        assert_eq!(bitset.get(3), true);
    }

    #[test]
    fn test_bitset_shift() {
        const TEST_BITS: usize = u128::BITS as usize;
        const TEST_PAGES: usize = TEST_BITS / PAGE_BITS;

        // Load our structure
        let o_page: u128 = 0xF4F1748182F917A1293FA11283;
        let o_bitset = {
            let mut bs = BitSet::new(TEST_BITS);

            // Load page by page our enormous u128 bitset
            for ind in 0..bs.len() {
                bs.put(
                    ind,
                    (o_page >> ((TEST_PAGES - 1) - ind) * PAGE_BITS) as BitPage,
                );
            }

            bs
        };

        // For every shift amount
        for shift_by in [1, 2, 3, 7, 50, 90, 129] {
            // Shift our 2 structures
            let page = o_page.unbounded_shr(shift_by);
            let mut bitset = o_bitset.clone();
            bitset.shr(shift_by as _);

            // Then compare them page by page
            for ind in 0..TEST_PAGES {
                let page_a = bitset.read(ind);
                let page_b = (page >> ((TEST_PAGES - 1) - ind) * PAGE_BITS) as BitPage;

                assert_eq!(page_a, page_b);
            }
        }
    }

    #[test]
    fn test_ack_window() {
        let mut window = SlidingAckWindow::new(128);

        // Make a zero packet. It's not yet marked
        assert!(!window.is_marked(0));

        // Mark it
        window.mark(0);

        // Our window has now shifted
        assert!(window.window_position() == 1);

        // And our packet is now too old
        assert!(window.is_old(0));

        // Let's mark some more packets
        for p in 2..16 {
            assert!(!window.is_marked(p));
            window.mark(p);

            assert!(window.is_marked(p));
        }

        // Still, our window position is at 1, because 1 isn't yet acknowledged
        assert!(window.window_position() == 1);

        // Because of which, we can't acknowledge higher packets
        assert!(window.is_out_of_reach(129));

        assert!(!window.is_marked(1));

        // Now let's finally mark this 1 packet
        window.mark(1);

        // Now, all these packets are now longer marked
        for p in 1..16 {
            assert!(window.is_old(p));
        }

        // And higher packets can finally flow
        assert!(!window.is_out_of_reach(129));
    }
}
