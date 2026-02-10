use crate::packet::PacketSeqId;

type BitPage = u64;

/// The amount of bits in our bit page
const PAGE_BITS: usize = BitPage::BITS as _;

/// A super minimal bitset implementation which allows setting bits at arbitrary positions and shifting the entire structure to the right 
#[derive(Clone)]
pub struct BitSet {
    pages: Box<[BitPage]>,
    len: usize,
}

impl BitSet {
    pub fn new(len: usize) -> Self {
        assert!(len > 0, "A bit array can't have zero frames");

        let pages = vec![0; len].into_boxed_slice();
        
        Self {
            len,
            pages
        }
    }

    /// Set a bit at the provided location
    pub fn set(&mut self, bind: usize, to: bool) {
        let value = self.get(bind);

        let ind = bind / PAGE_BITS;
        let offset = bind % PAGE_BITS;
        
        if value != to {
            self.pages[ind] ^= 1 << ((PAGE_BITS-1)-offset); 
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
        assert!(index/PAGE_BITS < self.len);

        let offset = index % PAGE_BITS;

        (
            self.pages[index/PAGE_BITS] & (1 << ((PAGE_BITS-1)-offset))
        ) > 0
    }

    /// The length of this bitset in bits
    pub fn bit_len(&self) -> usize {
        self.len * PAGE_BITS
    }

    /// Shift this structure to the right
    pub fn shr(&mut self, mut by: usize) {

        // When we're shifting pages, we can shift by entire integers at once. This is a pretty slow operation, 
        if by >= PAGE_BITS {

            // By how many pages to shift
            let shift_pages = by / PAGE_BITS;

            // Decrement by an entire page
            by -= shift_pages * PAGE_BITS;
            
            // For each page index, starting from 1
            for ind in (0..self.len).rev() {
                let new_ind = ind + shift_pages;

                if new_ind >= self.len {
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
        for i in (0..self.len).rev() {

            // If it's the last page - simply shift it 
            if i == self.len-1 {
                self.pages[i] >>= by;
            } else {
                // In any other case we're going to shift our current page onto the right one
                self.pages[i+1] |= self.pages[i] << (PAGE_BITS-by);

                // And now shift our own page
                self.pages[i] >>= by;
            }
        }
    }

    pub fn as_ref(&self) -> &[BitPage] {
        &self.pages
    }
}

/// The sliding window structure that helps keeping track of acknowledged packets.
/// 
/// The way it works, is the window contains the general offset (the latest packet), and frames that go **before** this offset packet. It can be
/// visualised like so: (latest)[frame_1][frame_2][frame_3][frame_n]
/// 
/// When we add a new packet, we compare it against our latest packet. 
/// - If it's larger - we have to shift our entire structure to the left, based on the
///   delta between the new packet and the latest one. After that we must mark our former packet the same way.
/// - If it's smaller - we must mark a bit in one of the available windows. If it's further than that - we won't mark anything.
/// - In any other case we don't do anything. 
pub struct SlidingAckWindow {
    /// The latest packet to arrive
    latest: Option<PacketSeqId>,
    
    /// The amount of packet frames (a single frame can contain multiple packets)
    frames_amount: usize,

    /// The frame storage itself (has a constant size)
    frames: BitSet
}

/// The mark of a packet, which describes its status in the packet window
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketMark {
    /// The packet is new (out of window bounds)
    New,

    /// The packet is within the bounds (marked)
    Marked,

    /// The packet is within the bounds (not marked)
    NonMarked,

    /// THe packet is old (out of bounds)
    Old
}

impl SlidingAckWindow {

    /// Create a new sliding window with the provided amount of frames. 
    /// 
    /// Note that a single frame contains multiple packets (64 to be precise)
    pub fn new(frames_amount: usize) -> Self {
        let frames = BitSet::new(frames_amount);

        Self { 
            latest: None, 
            frames_amount, 
            frames
        }
    }

    /// Mark this packet
    pub fn mark(&mut self, packet: PacketSeqId) {
        
        match self.latest {
            // We don't have any packets, so this one is the first we encounter
            None => {
                self.latest = Some(packet);
                // Our first bit is then marked as our most recent packet
                self.frames.set(0, true);
            },

            // We already have a packet, so we must test against it
            Some(latest) => {

                // If our packet is more recent than the latest - it automatically shifts our entire structure
                if packet > latest {

                    // Compute the delta
                    let delta = packet-latest;
                    
                    // Shift our bits to the right
                    self.frames.shr(delta as usize);

                    // Mark this new packet right at the top
                    self.frames.set(0, true);

                    // This packet now is the latest one
                    self.latest = Some(packet);
                } else {
                    // In any other case the packet is probably in our window

                    // Compute its bit-index
                    let bind = (latest-packet) as usize;

                    // If it's in our window range - mark it
                    if bind < self.frames.bit_len() {
                        self.frames.set(bind, true);
                    }
                }
            }
        }

        if self.latest.is_none() {
            self.latest = Some(packet);
        }
    }

    /// Get the mark status for the provided packet
    pub fn get_marked(&self, packet: PacketSeqId) -> PacketMark {
        match self.latest {
            // We don't have any data whatsoever, so the packet is new
            None => PacketMark::New,

            Some(latest) => {

                if packet > latest {
                    // The packet is newer than our latest one
                    PacketMark::New
                } else {
                    // In any other case we must compute a delta between these packets
                    let delta = (latest-packet) as usize;
                    println!("Delta: {delta}");

                    if delta < self.frames.bit_len() {
                        // Check if it's marked
                        let marked = self.frames.get(delta);
                        if marked {
                            println!("Marked: {marked}");
                            PacketMark::Marked
                        } else {
                            PacketMark::NonMarked
                        }
                    } else {
                        // In any other case it's out of reach, so it probably was marked, but no longer
                        PacketMark::Old
                    }
                }
            }
        }
    }

    /// The packet is considered "new" if it's newer or wasn't marked within the window bounds
    pub fn is_new(&self, packet: PacketSeqId) -> bool {        
        let mark = self.get_marked(packet);

        // Our packet is new if it's old or non-marked (within window bounds)        
        mark == PacketMark::New || mark == PacketMark::NonMarked
    }

    /// Check if this packet is old (no longer within the window bounds)
    pub fn is_old(&self, packet: PacketSeqId) -> bool {
        self.get_marked(packet) == PacketMark::Old
    }
}

#[cfg(test)]
mod tests {
    use crate::window::{BitPage, BitSet, PAGE_BITS, SlidingAckWindow};


    #[test]
    fn test_bitset_set() {
        let mut bitset = BitSet::new(4);

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
        const TEST_PAGES: usize = (u128::BITS as usize) / PAGE_BITS;

        // Load our structure
        let o_page: u128 = 0xF4F1748182F917A1293FA11283;
        let o_bitset = {   
            let mut bs = BitSet::new(TEST_PAGES);

            // Load page by page our enormous u128 bitset
            for ind in 0..TEST_PAGES {
                bs.put(ind, (o_page >> ((TEST_PAGES-1)-ind)*PAGE_BITS) as BitPage);
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
                let page_b = (page >> ((TEST_PAGES-1)-ind)*PAGE_BITS) as BitPage;

                assert_eq!(page_a, page_b);
            }
        }
    }

    #[test]
    fn test_ack_window() {
        let mut window = SlidingAckWindow::new(2);

        // Make a zero packet. It's not yet marked
        let a = 0;
        assert!(window.is_new(a));

        // Mark it
        window.mark(a);
        assert!(!window.is_new(a));

        // Let's mark some more packets
        for p in 1..16 {
            assert!(window.is_new(p));
            window.mark(p);

            assert!(!window.is_new(p));
        }

        // Now we're going to mark 128, which will destroy the oldest packet we got (0)
        window.mark(128);

        assert!(window.is_old(a));

        // However, all the other packets must still be present
        for p in 1..16 {
            assert!(!window.is_new(p));
        }

        // Let's mark a more recent one
        window.mark(144);

        // These now should be obsolete
        for p in 1..16 {
            assert!(window.is_old(p));
        }
    }
}
