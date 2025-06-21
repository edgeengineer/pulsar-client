/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Foundation

/// Schema definition for Boolean messages
public struct BooleanSchema: SchemaProtocol {
    public typealias T = Bool
    
    public let schemaInfo: SchemaInfo
    
    private static let trueValue = Data([1])
    private static let falseValue = Data([0])
    
    public init() {
        self.schemaInfo = SchemaInfo(
            name: "Boolean",
            type: .boolean,
            schema: nil,
            properties: [:]
        )
    }
    
    public func encode(_ value: Bool) throws -> Data {
        return value ? Self.trueValue : Self.falseValue
    }
    
    public func decode(_ data: Data) throws -> Bool {
        guard data.count == 1 else {
            throw PulsarClientError.schemaSerializationFailed(
                "BooleanSchema expected to decode 1 byte, but received \(data.count) bytes"
            )
        }
        return data[0] != 0
    }
}