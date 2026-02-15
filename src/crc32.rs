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

/// Compute an IEEE CRC32 checksum on the provided slice of bytes using a pre-generated table
///
/// This checksum can be then embedded in your binary. Note that you shouldn't compute a checksum of an augmented binary (original binary + checksum).
/// Instead, you separately compute a checksum of your received binary, and ONLY THEN you check this checksum against the one you received.
///
/// In case we get `[checksum][data]`, we must separately verify that `crc32(&data) == checksum`.
pub fn crc32(data: &[u8]) -> u32 {
    // The initialisation value used in the IEEE CRC32 implementation
    const INIT: u32 = 0xFFFFFFFF;

    // Initialise our remainder
    let mut remainder: u32 = INIT;

    // For byte
    for chunk in data.iter().copied() {
        // Compute an index from our remainder+chunk (^ to avoid carries)
        let ind = ((remainder as u8) ^ chunk) as usize;

        // Finally, move the byte out and add our precomputed CRC value to the remainder
        remainder = (remainder >> 8) ^ CRC32_TABLE[ind];
    }

    // Finally, return the value by XORing it with the init value
    remainder ^ INIT
}

#[cfg(test)]
mod tests {
    use super::crc32;

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
}
