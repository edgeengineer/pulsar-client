import Foundation
import Atomics
import Metrics
import Logging

/// PulsarMeter provides metrics collection for the Pulsar client
public final class PulsarMeter: @unchecked Sendable {
    
    /// Shared instance for global metrics collection
    public static let shared = PulsarMeter()
    
    private let logger = Logger(label: "PulsarMeter")
    
    // MARK: - Atomic Counters
    
    private let clientCount = ManagedAtomic<Int>(0)
    private let connectionCount = ManagedAtomic<Int>(0)
    private let producerCount = ManagedAtomic<Int>(0)
    private let consumerCount = ManagedAtomic<Int>(0)
    private let readerCount = ManagedAtomic<Int>(0)
    
    // MARK: - Metrics Configuration
    
    private let isEnabled = ManagedAtomic<Bool>(false)
    private var clientName: String = "PulsarClient"
    private var clientVersion: String = "1.0.0"
    
    // MARK: - Histogram Recorders
    
    private lazy var producerSendDurationRecorder = Timer(
        label: "pulsar.producer.send.duration",
        dimensions: [("unit", "milliseconds")]
    )
    
    private lazy var consumerProcessDurationRecorder = Timer(
        label: "pulsar.consumer.process.duration",
        dimensions: [("unit", "milliseconds")]
    )
    
    // MARK: - Gauges
    
    private var gaugesInitialized = false
    private let initializationLock = NSLock()
    
    // MARK: - Initialization
    
    private init() {
        setupClientInfo()
    }
    
    private func setupClientInfo() {
        if let bundle = Bundle.main.infoDictionary {
            clientName = bundle["CFBundleName"] as? String ?? "PulsarClient"
            clientVersion = bundle["CFBundleShortVersionString"] as? String ?? "1.0.0"
        }
    }
    
    // MARK: - Configuration
    
    /// Enable metrics collection
    public func enable() {
        isEnabled.store(true, ordering: .relaxed)
        initializeGauges()
        logger.info("Metrics collection enabled")
    }
    
    /// Disable metrics collection
    public func disable() {
        isEnabled.store(false, ordering: .relaxed)
        logger.info("Metrics collection disabled")
    }
    
    /// Check if metrics collection is enabled
    public var metricsEnabled: Bool {
        isEnabled.load(ordering: .relaxed)
    }
    
    private func initializeGauges() {
        initializationLock.lock()
        defer { initializationLock.unlock() }
        
        guard !gaugesInitialized else { return }
        
        // Note: Gauges in swift-metrics are not observable by default
        // We'll update gauge values when counts change instead
        // This is a limitation of the current swift-metrics API
        
        gaugesInitialized = true
        logger.debug("Metrics gauges initialized")
    }
    
    // MARK: - Client Metrics
    
    public func clientCreated() {
        let newCount = clientCount.wrappingIncrementThenLoad(ordering: .relaxed)
        if metricsEnabled {
            Gauge(label: "pulsar.client.count").record(Double(newCount))
        }
        logger.trace("Client created, count: \(newCount)")
    }
    
    public func clientDisposed() {
        let newCount = clientCount.wrappingDecrementThenLoad(ordering: .relaxed)
        if metricsEnabled {
            Gauge(label: "pulsar.client.count").record(Double(newCount))
        }
        logger.trace("Client disposed, count: \(newCount)")
    }
    
    public var numberOfClients: Int {
        clientCount.load(ordering: .relaxed)
    }
    
    // MARK: - Connection Metrics
    
    public func connectionCreated() {
        let newCount = connectionCount.wrappingIncrementThenLoad(ordering: .relaxed)
        if metricsEnabled {
            Gauge(label: "pulsar.connection.count").record(Double(newCount))
        }
        logger.trace("Connection created, count: \(newCount)")
    }
    
    public func connectionDisposed() {
        let newCount = connectionCount.wrappingDecrementThenLoad(ordering: .relaxed)
        if metricsEnabled {
            Gauge(label: "pulsar.connection.count").record(Double(newCount))
        }
        logger.trace("Connection disposed, count: \(newCount)")
    }
    
    public var numberOfConnections: Int {
        connectionCount.load(ordering: .relaxed)
    }
    
    // MARK: - Producer Metrics
    
    public func producerCreated() {
        let newCount = producerCount.wrappingIncrementThenLoad(ordering: .relaxed)
        if metricsEnabled {
            Gauge(label: "pulsar.producer.count").record(Double(newCount))
        }
        logger.trace("Producer created, count: \(newCount)")
    }
    
    public func producerDisposed() {
        let newCount = producerCount.wrappingDecrementThenLoad(ordering: .relaxed)
        if metricsEnabled {
            Gauge(label: "pulsar.producer.count").record(Double(newCount))
        }
        logger.trace("Producer disposed, count: \(newCount)")
    }
    
    public var numberOfProducers: Int {
        producerCount.load(ordering: .relaxed)
    }
    
    // MARK: - Consumer Metrics
    
    public func consumerCreated() {
        let newCount = consumerCount.wrappingIncrementThenLoad(ordering: .relaxed)
        if metricsEnabled {
            Gauge(label: "pulsar.consumer.count").record(Double(newCount))
        }
        logger.trace("Consumer created, count: \(newCount)")
    }
    
    public func consumerDisposed() {
        let newCount = consumerCount.wrappingDecrementThenLoad(ordering: .relaxed)
        if metricsEnabled {
            Gauge(label: "pulsar.consumer.count").record(Double(newCount))
        }
        logger.trace("Consumer disposed, count: \(newCount)")
    }
    
    public var numberOfConsumers: Int {
        consumerCount.load(ordering: .relaxed)
    }
    
    // MARK: - Reader Metrics
    
    public func readerCreated() {
        let newCount = readerCount.wrappingIncrementThenLoad(ordering: .relaxed)
        if metricsEnabled {
            Gauge(label: "pulsar.reader.count").record(Double(newCount))
        }
        logger.trace("Reader created, count: \(newCount)")
    }
    
    public func readerDisposed() {
        let newCount = readerCount.wrappingDecrementThenLoad(ordering: .relaxed)
        if metricsEnabled {
            Gauge(label: "pulsar.reader.count").record(Double(newCount))
        }
        logger.trace("Reader disposed, count: \(newCount)")
    }
    
    public var numberOfReaders: Int {
        readerCount.load(ordering: .relaxed)
    }
    
    // MARK: - Duration Metrics
    
    /// Record the duration of a producer send operation
    public func recordSendDuration(
        startTime: Date,
        topic: String? = nil,
        producerName: String? = nil,
        partition: Int? = nil
    ) {
        guard metricsEnabled else { return }
        
        let duration = Date().timeIntervalSince(startTime)
        
        var dimensions: [(String, String)] = []
        if let topic = topic {
            dimensions.append(("topic", topic))
        }
        if let producerName = producerName {
            dimensions.append(("producer", producerName))
        }
        if let partition = partition {
            dimensions.append(("partition", String(partition)))
        }
        
        producerSendDurationRecorder.recordNanoseconds(Int64(duration * 1_000_000_000))
        
        logger.trace("Recorded send duration: \(duration * 1000)ms", metadata: [
            "topic": .string(topic ?? "unknown"),
            "duration_ms": .stringConvertible(duration * 1000)
        ])
    }
    
    /// Record the duration of a consumer message processing operation
    public func recordProcessDuration(
        startTime: Date,
        topic: String? = nil,
        subscription: String? = nil,
        consumerName: String? = nil
    ) {
        guard metricsEnabled else { return }
        
        let duration = Date().timeIntervalSince(startTime)
        
        var dimensions: [(String, String)] = []
        if let topic = topic {
            dimensions.append(("topic", topic))
        }
        if let subscription = subscription {
            dimensions.append(("subscription", subscription))
        }
        if let consumerName = consumerName {
            dimensions.append(("consumer", consumerName))
        }
        
        consumerProcessDurationRecorder.recordNanoseconds(Int64(duration * 1_000_000_000))
        
        logger.trace("Recorded process duration: \(duration * 1000)ms", metadata: [
            "topic": .string(topic ?? "unknown"),
            "subscription": .string(subscription ?? "unknown"),
            "duration_ms": .stringConvertible(duration * 1000)
        ])
    }
    
    // MARK: - Utility Methods
    
    /// Record a custom counter metric
    public func recordCounter(
        _ name: String,
        value: Int64 = 1,
        dimensions: [(String, String)] = []
    ) {
        guard metricsEnabled else { return }
        
        Counter(label: "pulsar.\(name)", dimensions: dimensions)
            .increment(by: value)
    }
    
    /// Record a custom gauge metric
    public func recordGauge(
        _ name: String,
        value: Double,
        dimensions: [(String, String)] = []
    ) {
        guard metricsEnabled else { return }
        
        Gauge(label: "pulsar.\(name)", dimensions: dimensions)
            .record(value)
    }
    
    /// Record a custom histogram metric
    public func recordHistogram(
        _ name: String,
        value: Double,
        dimensions: [(String, String)] = [],
        unit: String? = nil
    ) {
        guard metricsEnabled else { return }
        
        var dims = dimensions
        if let unit = unit {
            dims.append(("unit", unit))
        }
        
        Recorder(label: "pulsar.\(name)", dimensions: dims)
            .record(value)
    }
    
    // MARK: - Metrics Summary
    
    /// Get a summary of current metrics
    public var metricsSummary: [String: Any] {
        [
            "enabled": metricsEnabled,
            "clients": numberOfClients,
            "connections": numberOfConnections,
            "producers": numberOfProducers,
            "consumers": numberOfConsumers,
            "readers": numberOfReaders,
            "client_name": clientName,
            "client_version": clientVersion
        ]
    }
}

// MARK: - Helper Extensions

public extension PulsarMeter {
    
    /// A timer context for measuring durations
    struct TimerContext {
        let startTime: Date
        private let recorder: (Date) -> Void
        
        init(recorder: @escaping (Date) -> Void) {
            self.startTime = Date()
            self.recorder = recorder
        }
        
        func stop() {
            recorder(startTime)
        }
    }
    
    /// Start a timer for send operations
    func startSendTimer(topic: String, producerName: String? = nil) -> TimerContext {
        TimerContext { [weak self] startTime in
            self?.recordSendDuration(
                startTime: startTime,
                topic: topic,
                producerName: producerName
            )
        }
    }
    
    /// Start a timer for process operations
    func startProcessTimer(topic: String, subscription: String, consumerName: String? = nil) -> TimerContext {
        TimerContext { [weak self] startTime in
            self?.recordProcessDuration(
                startTime: startTime,
                topic: topic,
                subscription: subscription,
                consumerName: consumerName
            )
        }
    }
}