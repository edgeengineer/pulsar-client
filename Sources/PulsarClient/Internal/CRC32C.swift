import Foundation

/// CRC32C (Castagnoli) implementation for Pulsar protocol
/// Mirrors DotPulsar's optimized slice-by-16 implementation
/// Uses polynomial 0x1EDC6F41 (reversed: 0x82F63B78)
enum CRC32C {
    // CRC32C polynomial (reversed)
    private static let generator: UInt32 = 0x82F63B78
    
    // Lookup table for CRC32C (16 * 256 entries for slice-by-16 algorithm)
    private static let lookup: [UInt32] = {
        var lookup = [UInt32](repeating: 0, count: 16 * 256)
        
        for i in 0..<256 {
            var entry = UInt32(i)
            
            for j in 0..<16 {
                for _ in 0..<8 {
                    if (entry & 1) == 1 {
                        entry = generator ^ (entry >> 1)
                    } else {
                        entry = entry >> 1
                    }
                }
                lookup[j * 256 + Int(i)] = entry
            }
        }
        
        return lookup
    }()
    
    /// Calculate CRC32C checksum
    /// - Parameter data: The data to checksum
    /// - Returns: The CRC32C checksum
    static func checksum(_ data: Data) -> UInt32 {
        var block = [UInt32](repeating: 0, count: 16)
        var checksum = UInt32.max
        var remainingBytes = data.count
        var readingBlock = remainingBytes >= 16
        var offset = 15
        
        for byte in data {
            if !readingBlock {
                checksum = lookup[Int(UInt8(truncatingIfNeeded: checksum ^ UInt32(byte)))] ^ (checksum >> 8)
                continue
            }
            
            let offsetBase = offset * 256
            
            if offset > 11 {
                block[offset] = lookup[offsetBase + Int(UInt8(truncatingIfNeeded: (checksum >> (8 * (15 - offset))) ^ UInt32(byte)))]
            } else {
                block[offset] = lookup[offsetBase + Int(byte)]
            }
            
            remainingBytes -= 1
            
            if offset == 0 {
                offset = 15
                readingBlock = remainingBytes >= 16
                checksum = 0
                
                for j in 0..<block.count {
                    checksum ^= block[j]
                }
            } else {
                offset -= 1
            }
        }
        
        return checksum ^ UInt32.max
    }
    
    /// Calculate CRC32C checksum for bytes
    /// - Parameter bytes: The bytes to checksum
    /// - Returns: The CRC32C checksum
    static func checksum<T: Sequence>(bytes: T) -> UInt32 where T.Element == UInt8 {
        var block = [UInt32](repeating: 0, count: 16)
        var checksum = UInt32.max
        var remainingBytes = 0
        
        // First, count the bytes
        for _ in bytes {
            remainingBytes += 1
        }
        
        var readingBlock = remainingBytes >= 16
        var offset = 15
        
        for byte in bytes {
            if !readingBlock {
                checksum = lookup[Int(UInt8(truncatingIfNeeded: checksum ^ UInt32(byte)))] ^ (checksum >> 8)
                continue
            }
            
            let offsetBase = offset * 256
            
            if offset > 11 {
                block[offset] = lookup[offsetBase + Int(UInt8(truncatingIfNeeded: (checksum >> (8 * (15 - offset))) ^ UInt32(byte)))]
            } else {
                block[offset] = lookup[offsetBase + Int(byte)]
            }
            
            remainingBytes -= 1
            
            if offset == 0 {
                offset = 15
                readingBlock = remainingBytes >= 16
                checksum = 0
                
                for j in 0..<block.count {
                    checksum ^= block[j]
                }
            } else {
                offset -= 1
            }
        }
        
        return checksum ^ UInt32.max
    }
}