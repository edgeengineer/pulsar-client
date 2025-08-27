import Foundation
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

// MARK: - Pulsar-specific ByteBuffer Extensions

extension ByteBuffer {
  
  // MARK: - Big Endian Integer Operations
  
  /// Write UInt32 in big endian
  @discardableResult
  public mutating func writeUInt32BE(_ value: UInt32) -> Int {
    return writeInteger(value, endianness: .big)
  }
  
  /// Read UInt32 in big endian
  public mutating func readUInt32BE() -> UInt32? {
    return readInteger(endianness: .big, as: UInt32.self)
  }
  
  // MARK: - Data Conversion
  
  /// Create a ByteBuffer from Data
  public static func from(_ data: Data) -> ByteBuffer {
    var buffer = ByteBufferAllocator().buffer(capacity: data.count)
    buffer.writeBytes(data)
    return buffer
  }
  
  /// Convert ByteBuffer to Data
  public func toData() -> Data {
    var copy = self
    return copy.readData(length: copy.readableBytes) ?? Data()
  }
  
  /// Read data of specified length
  public mutating func readData(length: Int) -> Data? {
    guard let bytes = readBytes(length: length) else { return nil }
    return Data(bytes)
  }
  
  /// Get data at specific position without moving reader index
  public func getData(at index: Int, length: Int) -> Data? {
    guard let bytes = getBytes(at: index, length: length) else { return nil }
    return Data(bytes)
  }
}