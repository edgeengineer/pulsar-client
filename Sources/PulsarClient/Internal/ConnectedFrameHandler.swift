import Foundation
import Logging
import NIO
import NIOCore

/// A simplified handler that manages the Pulsar connection handshake
/// It blocks channelActive from propagating until CONNECTED is received
final class ConnectedFrameHandler: ChannelDuplexHandler, @unchecked Sendable {
    typealias InboundIn = PulsarFrame
    typealias InboundOut = PulsarFrame
    typealias OutboundIn = PulsarFrame
    typealias OutboundOut = PulsarFrame
    
    private let logger: Logger
    private let connectCommand: Pulsar_Proto_BaseCommand
    private let handshakeTimeout: TimeAmount
    
    private var handshakeCompleted = false
    private var handshakePromise: EventLoopPromise<Void>?
    private var timeoutTask: Scheduled<Void>?
    
    // Public property to check if handshake completed successfully
    var isHandshakeComplete: Bool {
        handshakeCompleted
    }
    
    init(
        connectCommand: Pulsar_Proto_BaseCommand,
        handshakeTimeout: TimeAmount = TimeAmount.seconds(10),
        logger: Logger
    ) {
        self.connectCommand = connectCommand
        self.handshakeTimeout = handshakeTimeout
        self.logger = logger
    }
    
    // MARK: - Inbound Handler
    
    func channelActive(context: ChannelHandlerContext) {
        logger.debug("Channel active, initiating Pulsar handshake")
        
        // Store context reference for use in callbacks
        // We'll store it in a way that avoids sendability issues
        let channel = context.channel
        let eventLoop = context.eventLoop
        
        // Create promise for handshake completion
        handshakePromise = eventLoop.makePromise(of: Void.self)
        
        // Set up timeout for handshake
        timeoutTask = eventLoop.scheduleTask(in: handshakeTimeout) { [handshakeCompleted, logger, handshakePromise] in
            if !handshakeCompleted {
                logger.error("Handshake timeout after \(self.handshakeTimeout) seconds")
                handshakePromise?.fail(PulsarClientError.timeout("CONNECTED response timeout"))
                channel.close(promise: nil)
            }
        }
        
        // Send CONNECT command
        let frame = PulsarFrame(command: connectCommand)
        context.writeAndFlush(self.wrapOutboundOut(frame), promise: nil)
        logger.debug("Sent CONNECT command")
        
        // Don't propagate channelActive until handshake completes
        // This ensures the Connection doesn't start processing until we're ready
        handshakePromise?.futureResult.whenSuccess { [logger] in
            logger.debug("Handshake completed, propagating channelActive")
        }
        
        handshakePromise?.futureResult.whenFailure { [logger, channel] error in
            logger.error("Handshake failed", metadata: ["error": "\(error)"])
            // Fire error and close channel
            channel.close(promise: nil)
        }
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = unwrapInboundIn(data)
        
        // Check if this is the CONNECTED response we're waiting for
        if !handshakeCompleted && frame.command.type == .connected {
            logger.debug(
                "Received CONNECTED response",
                metadata: [
                    "serverVersion": "\(frame.command.connected.serverVersion)",
                    "protocolVersion": "\(frame.command.connected.protocolVersion)"
                ]
            )
            
            // Cancel timeout
            timeoutTask?.cancel()
            timeoutTask = nil
            
            // Mark handshake as completed
            handshakeCompleted = true
            
            // Complete the handshake promise
            handshakePromise?.succeed(())
            handshakePromise = nil
            
            // Don't forward the CONNECTED frame - it was just for handshake
            return
        }
        
        // For all other frames (or CONNECTED after handshake), pass them through
        if handshakeCompleted {
            context.fireChannelRead(data)
        } else {
            // If we receive other frames before handshake completes, it's an error
            logger.error(
                "Received unexpected frame before handshake completed",
                metadata: ["frameType": "\(frame.command.type)"]
            )
            handshakePromise?.fail(PulsarClientError.connectionFailed("Unexpected frame before CONNECTED"))
            context.close(promise: nil)
        }
    }
    
    func channelInactive(context: ChannelHandlerContext) {
        // Clean up if channel becomes inactive
        timeoutTask?.cancel()
        if !handshakeCompleted {
            handshakePromise?.fail(PulsarClientError.connectionFailed("Channel closed before handshake"))
        }
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        logger.error("Error in ConnectedFrameHandler", metadata: ["error": "\(error)"])
        
        // Fail handshake if not completed
        if !handshakeCompleted {
            handshakePromise?.fail(error)
        }
    }
    
    // MARK: - Outbound Handler
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        // During handshake, only allow the initial CONNECT command
        if !handshakeCompleted {
            let frame = unwrapOutboundIn(data)
            if frame.command.type != .connect {
                logger.warning(
                    "Blocking outbound frame during handshake",
                    metadata: ["frameType": "\(frame.command.type)"]
                )
                promise?.fail(PulsarClientError.connectionFailed("Cannot send frames before handshake completes"))
                return
            }
        }
        
        // Pass through
        context.write(data, promise: promise)
    }
}