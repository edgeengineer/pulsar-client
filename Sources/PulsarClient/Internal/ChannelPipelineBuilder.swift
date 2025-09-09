import Foundation
import Logging
import NIO
import NIOCore
import NIOSSL

/// Utility for building Pulsar channel pipelines
internal enum ChannelPipelineBuilder {
  
  /// Configuration options for building a channel pipeline
  struct Configuration {
    let url: PulsarURL
    let includeRawDataLogger: Bool
    let connectCommand: Pulsar_Proto_BaseCommand?
    let logger: Logger?
    
    init(
      url: PulsarURL,
      includeRawDataLogger: Bool = false,
      connectCommand: Pulsar_Proto_BaseCommand? = nil,
      logger: Logger? = nil
    ) {
      self.url = url
      self.includeRawDataLogger = includeRawDataLogger
      self.connectCommand = connectCommand
      self.logger = logger
    }
  }
  
  /// Build the base pipeline handlers (SSL + Frame codec)
  /// This is the minimal pipeline needed for Pulsar communication
  static func buildBasePipeline(
    for url: PulsarURL,
    on eventLoop: EventLoop
  ) -> Result<[ChannelHandler], Error> {
    var handlers: [ChannelHandler] = []
    
    // Add SSL handler if needed
    if url.isSSL {
      do {
        let sslContext = try NIOSSLContext(configuration: .makeClientConfiguration())
        let sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: url.host)
        handlers.append(sslHandler)
      } catch {
        return .failure(error)
      }
    }
    
    // Add frame encoder/decoder
    handlers.append(ByteToMessageHandler(PulsarFrameByteDecoder()))
    handlers.append(MessageToByteHandler(PulsarFrameByteEncoder()))
    
    return .success(handlers)
  }
  
  /// Build a complete pipeline with optional components
  static func buildCompletePipeline(
    configuration: Configuration,
    on eventLoop: EventLoop
  ) -> Result<[ChannelHandler], Error> {
    var handlers: [ChannelHandler] = []
    
    // Add SSL handler if needed
    if configuration.url.isSSL {
      do {
        let sslContext = try NIOSSLContext(configuration: .makeClientConfiguration())
        let sslHandler = try NIOSSLClientHandler(
          context: sslContext, 
          serverHostname: configuration.url.host
        )
        handlers.append(sslHandler)
      } catch {
        return .failure(error)
      }
    }
    
    // Add raw data logger if requested (for debugging)
    if configuration.includeRawDataLogger {
      handlers.append(RawDataLogger())
    }
    
    // Add frame codec handlers
    handlers.append(ByteToMessageHandler(PulsarFrameByteDecoder()))
    handlers.append(MessageToByteHandler(PulsarFrameByteEncoder()))
    
    // Add connection handshake handler if connect command provided
    if let connectCommand = configuration.connectCommand,
       let logger = configuration.logger {
      let connectedHandler = ConnectedFrameHandler(
        connectCommand: connectCommand,
        handshakeTimeout: TimeAmount.seconds(10),
        logger: logger
      )
      handlers.append(connectedHandler)
    }
    
    return .success(handlers)
  }
  
  /// Setup channel pipeline with the given handlers
  static func setupPipeline(
    on channel: Channel,
    with handlers: [ChannelHandler]
  ) -> EventLoopFuture<Void> {
    return channel.pipeline.addHandlers(handlers, position: .last)
  }
  
  /// Convenience method to build and setup a base pipeline in one step
  static func setupBasePipeline(
    on channel: Channel,
    for url: PulsarURL
  ) -> EventLoopFuture<Void> {
    switch buildBasePipeline(for: url, on: channel.eventLoop) {
    case .success(let handlers):
      return setupPipeline(on: channel, with: handlers)
    case .failure(let error):
      return channel.eventLoop.makeFailedFuture(error)
    }
  }
  
  /// Convenience method to build and setup a complete pipeline in one step
  static func setupCompletePipeline(
    on channel: Channel,
    configuration: Configuration
  ) -> EventLoopFuture<Void> {
    switch buildCompletePipeline(configuration: configuration, on: channel.eventLoop) {
    case .success(let handlers):
      return setupPipeline(on: channel, with: handlers)
    case .failure(let error):
      return channel.eventLoop.makeFailedFuture(error)
    }
  }
}