#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import NIOCore
import SwiftProtobuf

extension ByteBuffer: @retroactive SwiftProtobufContiguousBytes {
    public func withUnsafeBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
        try self.withUnsafeReadableBytes(body)
    }

    public var count: Int { readableBytes }
    
    public init(_ sequence: some Sequence<UInt8>) {
        self.init(bytes: sequence)
    }

    public mutating func withUnsafeMutableBytes<R>(
        _ body: (UnsafeMutableRawBufferPointer) throws -> R
    ) rethrows -> R{
        try withUnsafeMutableReadableBytes { buffer in
            try body(buffer)
        }
    }
}
