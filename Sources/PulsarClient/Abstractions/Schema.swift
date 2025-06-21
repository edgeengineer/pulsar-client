import Foundation

/// Schema protocol for message serialization/deserialization
public protocol SchemaProtocol: Sendable {
    associatedtype T
    
    /// Schema type information
    var schemaInfo: SchemaInfo { get }
    
    /// Encode a value to data
    /// - Parameter value: The value to encode
    /// - Returns: The encoded data
    func encode(_ value: T) throws -> Data
    
    /// Decode data to a value
    /// - Parameter data: The data to decode
    /// - Returns: The decoded value
    func decode(_ data: Data) throws -> T
}

/// Concrete schema implementation
public struct Schema<T>: SchemaProtocol {
    public let schemaInfo: SchemaInfo
    private let encoder: @Sendable (T) throws -> Data
    private let decoder: @Sendable (Data) throws -> T
    
    public init(
        schemaInfo: SchemaInfo,
        encoder: @escaping @Sendable (T) throws -> Data,
        decoder: @escaping @Sendable (Data) throws -> T
    ) {
        self.schemaInfo = schemaInfo
        self.encoder = encoder
        self.decoder = decoder
    }
    
    public func encode(_ value: T) throws -> Data {
        try encoder(value)
    }
    
    public func decode(_ data: Data) throws -> T {
        try decoder(data)
    }
}

/// Schema information
public struct SchemaInfo: Sendable {
    public let name: String
    public let type: SchemaType
    public let schema: Data?
    public let properties: [String: String]
    
    public init(
        name: String,
        type: SchemaType,
        schema: Data? = nil,
        properties: [String: String] = [:]
    ) {
        self.name = name
        self.type = type
        self.schema = schema
        self.properties = properties
    }
}

/// Schema types
public enum SchemaType: String, Sendable {
    case none = "None"
    case string = "String"
    case json = "Json"
    case protobuf = "Protobuf"
    case avro = "Avro"
    case boolean = "Boolean"
    case int8 = "Int8"
    case int16 = "Int16"
    case int32 = "Int32"
    case int64 = "Int64"
    case float = "Float"
    case double = "Double"
    case date = "Date"
    case time = "Time"
    case timestamp = "Timestamp"
    case keyValue = "KeyValue"
    case bytes = "Bytes"
    case auto = "Auto"
    case autoConsume = "AutoConsume"
    case autoPublish = "AutoPublish"
}

/// Message schema definitions with built-in schemas
public enum Schemas {
    
    // MARK: - Primitive Schemas
    
    /// Raw bytes schema
    public static let bytes = BytesSchema()
    
    /// UTF-8 string schema
    public static let string = StringSchema()
    
    /// Boolean schema
    public static let boolean = BooleanSchema()
    
    /// Byte (Int8) schema
    public static let int8 = Int8Schema()
    
    /// Short (Int16) schema
    public static let int16 = Int16Schema()
    
    /// Integer (Int32) schema
    public static let int32 = Int32Schema()
    
    /// Long (Int64) schema
    public static let int64 = Int64Schema()
    
    /// Float schema
    public static let float = FloatSchema()
    
    /// Double schema
    public static let double = DoubleSchema()
    
    /// Date schema
    public static let date = DateSchema()
    
    /// Time schema (TimeInterval)
    public static let time = TimeSchema()
    
    /// Timestamp schema
    public static let timestamp = TimestampSchema()
    
    // MARK: - Complex Schemas
    
    /// JSON schema for Codable types
    public static func json<T: Codable>(_ type: T.Type) -> JSONSchema<T> {
        return JSONSchema<T>()
    }
    
    /// Auto-consume schema (determines schema from message)
    public static func autoConsume<T>() -> AutoConsumeSchema<T> {
        return AutoConsumeSchema<T>()
    }
    
    /// Auto-publish schema (determines schema from producer)
    public static func autoPublish<T>() -> AutoPublishSchema<T> {
        return AutoPublishSchema<T>()
    }
}

/// Built-in schemas - Legacy support
public extension Schema where T == Data {
    /// Bytes schema (no serialization)
    static var bytes: Schema<Data> {
        Schema<Data>(
            schemaInfo: SchemaInfo(name: "Bytes", type: .bytes),
            encoder: { $0 },
            decoder: { $0 }
        )
    }
}

public extension Schema where T == String {
    /// String schema (UTF-8)
    static var string: Schema<String> {
        Schema<String>(
            schemaInfo: SchemaInfo(name: "String", type: .string),
            encoder: { $0.data(using: .utf8) ?? Data() },
            decoder: { String(data: $0, encoding: .utf8) ?? "" }
        )
    }
}

public extension Schema {
    /// JSON schema for Codable types
    static func json<U: Codable>(_ type: U.Type) -> Schema<U> {
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()
        
        return Schema<U>(
            schemaInfo: SchemaInfo(name: String(describing: U.self), type: .json),
            encoder: { try encoder.encode($0) },
            decoder: { try decoder.decode(U.self, from: $0) }
        )
    }
}