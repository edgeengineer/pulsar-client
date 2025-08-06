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

/// Builder for creating producers
@dynamicMemberLookup
public final class ProducerBuilder<T> where T: Sendable {
    internal var options: ProducerOptions<T>
    
    public init(options: ProducerOptions<T>) {
        self.options = options
    }
    
    @discardableResult
    public func producerName(_ name: String?) -> Self {
        options = options.withProducerName(name)
        return self
    }
    
    @discardableResult
    public func initialSequenceId(_ id: UInt64) -> Self {
        options = options.withInitialSequenceId(id)
        return self
    }
    
    @discardableResult
    public func sendTimeout(_ timeout: TimeInterval) -> Self {
        options = options.withSendTimeout(timeout)
        return self
    }
    
    @discardableResult
    public func batchingEnabled(_ enabled: Bool) -> Self {
        options = options.withBatchingEnabled(enabled)
        return self
    }
    
    @discardableResult
    public func batchingMaxMessages(_ max: UInt) -> Self {
        options = options.withBatchingMaxMessages(max)
        return self
    }
    
    @discardableResult
    public func batchingMaxDelay(_ delay: TimeInterval) -> Self {
        options = options.withBatchingMaxDelay(delay)
        return self
    }
    
    @discardableResult
    public func batchingMaxBytes(_ bytes: UInt) -> Self {
        options = options.withBatchingMaxBytes(bytes)
        return self
    }
    
    @discardableResult
    public func compressionType(_ type: CompressionType) -> Self {
        options = options.withCompressionType(type)
        return self
    }
    
    @discardableResult
    public func hashingScheme(_ scheme: HashingScheme) -> Self {
        options = options.withHashingScheme(scheme)
        return self
    }
    
    @discardableResult
    public func messageRouter(_ router: MessageRouter) -> Self {
        options = options.withMessageRouter(router)
        return self
    }
    
    @discardableResult
    public func cryptoKeyReader(_ reader: CryptoKeyReader?) -> Self {
        options = options.withCryptoKeyReader(reader)
        return self
    }
    
    @discardableResult
    public func encryptionKeys(_ keys: [String]) -> Self {
        options = options.withEncryptionKeys(keys)
        return self
    }
    
    @discardableResult
    public func producerAccessMode(_ mode: ProducerAccessMode) -> Self {
        options = options.withProducerAccessMode(mode)
        return self
    }
    
    @discardableResult
    public func attachTraceInfoToMessages(_ enabled: Bool) -> Self {
        options = options.withAttachTraceInfoToMessages(enabled)
        return self
    }
    
    @discardableResult
    public func maxPendingMessages(_ max: UInt) -> Self {
        options = options.withMaxPendingMessages(max)
        return self
    }
    
    @discardableResult
    public func properties(_ properties: [String: String]) -> Self {
        options = options.withProducerProperties(properties)
        return self
    }
    
    @discardableResult
    public func stateChangedHandler(_ handler: @escaping @Sendable (ProducerStateChanged<T>) -> Void) -> Self {
        options = options.withStateChangedHandler(handler)
        return self
    }
    
    @discardableResult
    public func intercept(_ interceptors: any ProducerInterceptor<T>...) -> Self {
        options = options.withInterceptors(options.interceptors + interceptors)
        return self
    }
    
    @discardableResult
    public func intercept(_ interceptors: [any ProducerInterceptor<T>]) -> Self {
        options = options.withInterceptors(options.interceptors + interceptors)
        return self
    }
    
    // Dynamic member lookup for accessing options properties
    public subscript<Value>(dynamicMember keyPath: KeyPath<ProducerOptions<T>, Value>) -> Value {
        return options[keyPath: keyPath]
    }
}