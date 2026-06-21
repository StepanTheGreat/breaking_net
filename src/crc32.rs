//! For some reason finding a simple CRC32 implementation is not so straightforward today?? Well, maybe this file could be useful for some.
//!
//! If you're looking for some motivation yourself - check out [this link](https://en.wikipedia.org/wiki/Computation_of_cyclic_redundancy_checks#CRC-32_example)

/// The constantly generated CRC32 table
const CRC32_TABLE: [u32; 256] = crc32_table();

/// The length of the CRC32 signature (it's a 4 byte integer)
pub const CRC32_SIG_LEN: usize = 4;

/// We're going to make a constant function to generate a table of precomputed CRC32 outputs
/// for each possible byte value (256 different possible combinations)
const fn crc32_table() -> [u32; 256] {
    // The generator polynomial for CRC32 (reversed). You can find it here: https://en.wikipedia.org/wiki/Cyclic_redundancy_check
    const GENERATOR: u32 = 0xEDB88320;

    // Initialise the table
    let mut table = [0u32; 256];

    // For every possible byte value (unfortunately no for-loops in const functions...)
    let mut i = 0;
    while i < 256 {
        // Load our byte
        let mut remainder = i as u32;

        // For each bit
        let mut bit = 0;
        while bit < 8 {
            // If our remainder equation is LSB aligned
            if (remainder & 0b1) != 0 {
                // Shift it to the right and divide by the generator equation
                remainder = (remainder >> 1) ^ GENERATOR;
            } else {
                // In any other case just shift it to the right
                remainder >>= 1;
            }

            bit += 1;
        }

        // Finally, save our result into the table
        table[i] = remainder;

        i += 1;
    }

    table
}

/// Compute an IEEE CRC32 checksum on the provided slice of slices of bytes using a pre-generated table
///
/// This checksum can be then embedded in your binary. Note that you shouldn't compute a checksum of an augmented binary (original binary + checksum).
/// Instead, you separately compute a checksum of your received binary, and ONLY THEN you check this checksum against the one you received.
///
/// In case we get `[checksum][data]`, we must separately verify that `crc32(&data) == checksum`.
fn crc32_multi(data_slices: &[&[u8]]) -> u32 {
    // The initialisation value used in the IEEE CRC32 implementation
    const INIT: u32 = 0xFFFFFFFF;

    // Initialise our remainder
    let mut remainder: u32 = INIT;

    for data_slice in data_slices.iter() {
        // For byte
        for chunk in data_slice.iter().copied() {
            // Compute an index from our remainder+chunk (^ to avoid carries)
            let ind = ((remainder as u8) ^ chunk) as usize;

            // Finally, move the byte out and add our precomputed CRC value to the remainder
            remainder = (remainder >> 8) ^ CRC32_TABLE[ind];
        }
    }

    // Finally, return the value by XORing it with the init value
    remainder ^ INIT
}

/// Verify the CRC32 signed data slice. This expects data in the specified format:
/// `[..data][4 crc32 bytes]`
fn crc32_verify(data: &[u8], signature: Option<&str>) -> bool {
    if data.len() < CRC32_SIG_LEN {
        // Can't fit the CRC signature - automatically fail
        return false;
    }

    let data_crc_len = data.len();
    let data_len = data_crc_len - CRC32_SIG_LEN;
    let signature = signature.unwrap_or("");

    let crc_bytes = &data[data_len..data_crc_len];

    let actual_crc_bytes = crc32_multi(&[&data[..data_len], signature.as_bytes()]).to_be_bytes();

    crc_bytes == actual_crc_bytes
}

/// Take the provided mutable array and sign it with a CRC32 signature right at the end.
///
/// The slice taken must be a slice capable of fitting data + crc32 signature (+4 bytes). So the overall layout is:
/// `[..data][4 empty CRC32 bytes]`
///
/// The last 4 bytes will be overwritten with a CRC32 signature
pub fn crc32_sign(data: &mut [u8], signature: Option<&str>) {
    assert!(
        data.len() >= CRC32_SIG_LEN,
        "Can't sign the provided data slice, since it can't fit a CRC32 signature"
    );

    let data_crc_len = data.len();
    let data_len = data_crc_len - CRC32_SIG_LEN;

    let signature = signature.unwrap_or("");

    // Compute the signature of our data + signature
    let crc_bytes = crc32_multi(&[&data[..data_len], signature.as_bytes()]).to_be_bytes();

    // Embed it into the slice
    data[data_len..data_crc_len].copy_from_slice(&crc_bytes);
}

/// A CRC32 sign/verification structure. It's particularly useful as a component for signing/verifying arbitrary data.
pub struct CRC32 {
    /// Protocol's signature
    signature: &'static str,

    mtu: usize,

    // Temporary buffer for write operations
    buffer: Box<[u8]>,
}

impl CRC32 {
    /// Create a new CRC32 instance. Do mind that this structure doesn't allocate more than provided `mtu`, so with a buffer
    /// of size 1500, your real capacity will always be 1496, since the remaining bytes will be taken by the signature.
    pub fn new(mtu: usize, signature: &'static str) -> Self {
        Self {
            signature,
            mtu,
            buffer: vec![0u8; mtu].into_boxed_slice(),
        }
    }

    /// Sign the provided data.
    ///
    /// Returns [None] if the data's length exceeds buffer's + signature length (`MTU-signature_size`)
    pub fn sign(&mut self, data: &[u8]) -> Option<&[u8]> {
        if data.len() > self.mtu - CRC32_SIG_LEN {
            // Too much data, can't fit a signature
            return None;
        }

        let data_len = data.len();
        let data_crc_len = data_len + CRC32_SIG_LEN;

        // Copy the message to our buffer
        self.buffer[..data_len].copy_from_slice(data);

        // Sign it
        crc32_sign(&mut self.buffer[..data_crc_len], Some(self.signature));

        // Augment our data slice to account for our new signature
        Some(&self.buffer[..data_crc_len])
    }

    /// Validate the provided data and return [Some] if the data passed the signature check
    pub fn validate<'a>(&self, data: &'a [u8]) -> Option<&'a [u8]> {
        // We received less bytes than our CRC signature
        if data.len() < CRC32_SIG_LEN {
            return None;
        }

        // Signature mismatch, early return
        if !crc32_verify(data, Some(self.signature)) {
            return None;
        }

        // Read everything excluding the signature
        let data_len = data.len() - CRC32_SIG_LEN;

        Some(&data[..data_len])
    }
}

#[cfg(test)]
mod tests {
    use super::crc32_multi;

    fn crc32(data: &[u8]) -> u32 {
        crc32_multi(&[data])
    }

    /// A super minimal crc32 test
    #[test]
    fn test_crc32() {
        const TEST_PAIRS: &[(&[u8], u32)] = &[
            (b"", 0x0),
            (b"a", 0xe8b7be43),
            (b"abc", 0x352441c2),
            // Thanks to https://github.com/froydnj/ironclad for the tests vectors
            (b"abcdefghijklmnopqrstuvwxyz", 0x4c2750bd),
            (
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
                0x1fc2e6d2,
            ),
            (
                b"12345678901234567890123456789012345678901234567890123456789012345678901234567890",
                0x7ca94a72,
            ),
            (b"1234567890", 0x261daee5),
            (b"1234577890", 0x1b7d8755),
        ];

        for (input, output) in TEST_PAIRS {
            assert_eq!(crc32(&input), *output);
        }
    }

    /// Check if crc_multi produces valid conctenated results
    #[test]
    fn test_crc32_multi() {
        const TEST_PAIRS: &[(&[u8], &[&[u8]])] = &[
            (b"", &[b"", b""]),
            (b"a", &[b"", b"a"]),
            (b"abc", &[b"ab", b"c"]),
            (
                b"abcdefghijklmnopqrstuvwxyz",
                &[b"abcdefghij", b"klmnopqrstuvwxyz"],
            ),
        ];

        for (input_single, input_multi) in TEST_PAIRS {
            assert_eq!(crc32(input_single), crc32_multi(input_multi));
        }
    }
}
