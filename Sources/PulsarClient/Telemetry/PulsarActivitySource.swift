import Foundation
import Tracing
import ServiceContextModule
import Logging

/// PulsarActivitySource provides distributed tracing for the Pulsar client
public final class PulsarActivitySource: @unchecked Sendable {
    
    /// Shared instance for global tracing
    public static let shared = PulsarActivitySource()
    
    private let logger = Logger(label: "PulsarActivitySource")
    
    // MARK: - Constants
    
    internal enum TracingConstants {
        static let traceParentKey = "messaging.trace_parent"
        static let traceStateKey = "messaging.trace_state"
        static let conversationIdKey = "messaging.conversation_id"
        
        // Span attribute keys
        static let messagingSystem = "messaging.system"
        static let messagingDestination = "messaging.destination"
        static let messagingDestinationKind = "messaging.destination_kind"
        static let messagingOperation = "messaging.operation"
        static let messagingUrl = "messaging.url"
        static let messagingMessageId = "messaging.message_id"
        static let messagingPayloadSize = "messaging.message_payload_size_bytes"
        
        // Pulsar-specific attributes
        static let pulsarSubscription = "messaging.pulsar.subscription"
        static let pulsarProducerName = "messaging.pulsar.producer_name"
        static let pulsarConsumerName = "messaging.pulsar.consumer_name"
        static let pulsarPartition = "messaging.pulsar.partition"
        
        // Exception attributes
        static let exceptionType = "exception.type"
        static let exceptionMessage = "exception.message"
        static let exceptionStacktrace = "exception.stacktrace"
    }
    
    // MARK: - Configuration
    
    private var isEnabled = false
    private var attachTraceToMessages = false
    private var linkConsumerTraces = false
    
    // MARK: - Initialization
    
    private init() {}
    
    // MARK: - Configuration Methods
    
    /// Enable tracing
    public func enable(
        attachTraceToMessages: Bool = false,
        linkConsumerTraces: Bool = false
    ) {
        self.isEnabled = true
        self.attachTraceToMessages = attachTraceToMessages
        self.linkConsumerTraces = linkConsumerTraces
        logger.info("Tracing enabled", metadata: [
            "attachTraceToMessages": .stringConvertible(attachTraceToMessages),
            "linkConsumerTraces": .stringConvertible(linkConsumerTraces)
        ])
    }
    
    /// Disable tracing
    public func disable() {
        self.isEnabled = false
        logger.info("Tracing disabled")
    }
    
    // MARK: - Producer Activities
    
    /// Start a producer activity/span
    public func startProducerActivity(
        topic: String,
        producerName: String? = nil,
        partition: Int? = nil,
        messageKey: String? = nil,
        serviceURL: String
    ) -> Activity? {
        guard isEnabled else { return nil }
        
        let operationName = "\(topic) send"
        
        let span = InstrumentationSystem.tracer.startSpan(
            operationName,
            context: ServiceContext.current ?? ServiceContext.topLevel,
            ofKind: .producer
        )
        
        // Set standard attributes
        span.attributes[TracingConstants.messagingSystem] = "pulsar"
        span.attributes[TracingConstants.messagingDestination] = topic
        span.attributes[TracingConstants.messagingDestinationKind] = "topic"
        span.attributes[TracingConstants.messagingOperation] = "send"
        span.attributes[TracingConstants.messagingUrl] = serviceURL
        
        // Set Pulsar-specific attributes
        if let producerName = producerName {
            span.attributes[TracingConstants.pulsarProducerName] = producerName
        }
        if let partition = partition {
            span.attributes[TracingConstants.pulsarPartition] = partition
        }
        if let messageKey = messageKey {
            span.attributes["messaging.message_key"] = messageKey
        }
        
        logger.trace("Started producer activity", metadata: [
            "operation": .string(operationName),
            "topic": .string(topic)
        ])
        
        return Activity(span: span, operationName: operationName)
    }
    
    // MARK: - Consumer Activities
    
    /// Start a consumer activity/span
    public func startConsumerActivity(
        topic: String,
        subscription: String,
        consumerName: String? = nil,
        messageProperties: [String: String]? = nil,
        serviceURL: String
    ) -> Activity? {
        guard isEnabled else { return nil }
        
        let operationName = "\(topic) process"
        
        // Extract parent context if linking is enabled
        let context = ServiceContext.current ?? ServiceContext.topLevel
        var links: [SpanLink] = []
        
        if linkConsumerTraces, let properties = messageProperties {
            if let parentContext = extractTraceContext(from: properties) {
                links.append(SpanLink(context: parentContext, attributes: [:]))
            }
        }
        
        let span = InstrumentationSystem.tracer.startSpan(
            operationName,
            context: context,
            ofKind: .consumer
        )
        
        // Set standard attributes
        span.attributes[TracingConstants.messagingSystem] = "pulsar"
        span.attributes[TracingConstants.messagingDestination] = topic
        span.attributes[TracingConstants.messagingDestinationKind] = "topic"
        span.attributes[TracingConstants.messagingOperation] = "process"
        span.attributes[TracingConstants.messagingUrl] = serviceURL
        span.attributes[TracingConstants.pulsarSubscription] = subscription
        
        // Set Pulsar-specific attributes
        if let consumerName = consumerName {
            span.attributes[TracingConstants.pulsarConsumerName] = consumerName
        }
        
        logger.trace("Started consumer activity", metadata: [
            "operation": .string(operationName),
            "topic": .string(topic),
            "subscription": .string(subscription)
        ])
        
        return Activity(span: span, operationName: operationName)
    }
    
    // MARK: - Trace Context Propagation
    
    /// Inject trace context into message properties
    public func injectTraceContext(
        into properties: inout [String: String],
        from activity: Activity?
    ) {
        guard attachTraceToMessages,
              let activity = activity else { return }
        
        // Inject W3C Trace Context
        var carrier = MessagePropertiesCarrier(properties: properties)
        InstrumentationSystem.instrument.inject(
            activity.context,
            into: &carrier,
            using: W3CTraceContextPropagator()
        )
        properties = carrier.properties
        
        // Add conversation ID if available
        if let conversationId = activity.conversationId {
            properties[TracingConstants.conversationIdKey] = conversationId
        }
        
        logger.trace("Injected trace context into message properties")
    }
    
    /// Extract trace context from message properties
    public func extractTraceContext(
        from properties: [String: String]
    ) -> ServiceContext? {
        let carrier = MessagePropertiesCarrier(properties: properties)
        var context = ServiceContext.topLevel
        
        InstrumentationSystem.instrument.extract(
            carrier,
            into: &context,
            using: W3CTraceContextPropagator()
        )
        
        logger.trace("Extracted trace context from message properties")
        return context
    }
    
    // MARK: - Activity Management
    
    /// Complete an activity with success
    public func completeActivity(
        _ activity: Activity?,
        messageId: MessageId? = nil,
        payloadSize: Int? = nil
    ) {
        guard let activity = activity else { return }
        
        if let messageId = messageId {
            activity.span.attributes[TracingConstants.messagingMessageId] = messageId.description
        }
        
        if let payloadSize = payloadSize {
            activity.span.attributes[TracingConstants.messagingPayloadSize] = payloadSize
        }
        
        activity.span.setStatus(.init(code: .ok))
        activity.span.end()
        
        logger.trace("Completed activity successfully", metadata: [
            "operation": .string(activity.operationName)
        ])
    }
    
    /// Fail an activity with an error
    public func failActivity(
        _ activity: Activity?,
        error: Error
    ) {
        guard let activity = activity else { return }
        
        activity.span.setStatus(.init(code: .error, message: String(describing: error)))
        
        // Record exception details
        activity.span.recordError(error)
        
        activity.span.end()
        
        logger.trace("Failed activity with error", metadata: [
            "operation": .string(activity.operationName),
            "error": .string(String(describing: error))
        ])
    }
}

// MARK: - Activity

/// Represents an active tracing activity/span
public struct Activity {
    var span: any Span
    let operationName: String
    var conversationId: String?
    var context: ServiceContext
    
    init(span: any Span, operationName: String) {
        self.span = span
        self.operationName = operationName
        self.context = ServiceContext.current ?? ServiceContext.topLevel
    }
    
    /// Add a custom attribute to the activity
    public mutating func setAttribute(_ key: String, value: SpanAttributeConvertible) {
        span.attributes[key] = value
    }
    
    /// Add an event to the activity
    public func addEvent(_ name: String, attributes: SpanAttributes = [:]) {
        span.addEvent(SpanEvent(name: name, attributes: attributes))
    }
    
    /// Set the conversation ID for this activity
    public mutating func setConversationId(_ id: String) {
        self.conversationId = id
        span.attributes[PulsarActivitySource.TracingConstants.conversationIdKey] = id
    }
}

// MARK: - Message Properties Carrier

/// Carrier for injecting/extracting trace context from message properties
private struct MessagePropertiesCarrier {
    var properties: [String: String]
    
    init(properties: [String: String]) {
        self.properties = properties
    }
}

// MARK: - W3C Trace Context Propagator

/// W3C Trace Context propagator for trace context injection/extraction
private struct W3CTraceContextPropagator: Injector, Extractor {
    typealias Carrier = MessagePropertiesCarrier
    
    func inject(_ context: ServiceContext, into carrier: inout MessagePropertiesCarrier) {
        // This is a simplified implementation
        // In production, you would properly format the W3C trace context
        // For now, we'll just store a simple trace ID
        if let traceID = context.traceID {
            carrier.properties[PulsarActivitySource.TracingConstants.traceParentKey] = traceID
        }
    }
    
    func extract(from carrier: MessagePropertiesCarrier, into context: inout ServiceContext) {
        // This is a simplified implementation
        // In production, you would properly parse the W3C trace context
        if let traceParent = carrier.properties[PulsarActivitySource.TracingConstants.traceParentKey] {
            context.traceID = traceParent
        }
    }
    
    // Additional protocol requirements for Injector
    func inject(_ value: String, forKey key: String, into carrier: inout MessagePropertiesCarrier) {
        carrier.properties[key] = value
    }
    
    // Additional protocol requirements for Extractor
    func extract(key: String, from carrier: MessagePropertiesCarrier) -> String? {
        carrier.properties[key]
    }
}

// MARK: - ServiceContext Extensions

private extension ServiceContext {
    var traceID: String? {
        get { self[TraceIDKey.self] }
        set { self[TraceIDKey.self] = newValue }
    }
}

private enum TraceIDKey: ServiceContextKey {
    typealias Value = String
    static var defaultValue: String? { nil }
}