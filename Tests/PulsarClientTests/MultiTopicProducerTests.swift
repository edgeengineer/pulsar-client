import Foundation
import Testing

@testable import PulsarClient

@Suite("Multi-Topic Producer Tests")
struct MultiTopicProducerTests {
    
    // Mock TopicRouter for testing
    actor MockTopicRouter<T: Sendable>: TopicRouter {
        typealias MessageType = T
        
        private var selectedIndex = 0
        
        func selectTopic(for message: T, metadata: MessageMetadata, topics: [String]) async -> String {
            let topic = topics[selectedIndex % topics.count]
            selectedIndex += 1
            return topic
        }
    }
    
    @Test("RoundRobinTopicRouter Distribution")
    func testRoundRobinTopicRouter() async {
        let router = RoundRobinTopicRouter<String>()
        let topics = ["topic1", "topic2", "topic3"]
        let metadata = MessageMetadata()
        
        // Should distribute evenly in round-robin fashion
        let selected1 = await router.selectTopic(for: "message1", metadata: metadata, topics: topics)
        let selected2 = await router.selectTopic(for: "message2", metadata: metadata, topics: topics)
        let selected3 = await router.selectTopic(for: "message3", metadata: metadata, topics: topics)
        let selected4 = await router.selectTopic(for: "message4", metadata: metadata, topics: topics)
        
        #expect(selected1 == "topic1")
        #expect(selected2 == "topic2")
        #expect(selected3 == "topic3")
        #expect(selected4 == "topic1") // Should wrap around
    }
    
    @Test("RoundRobinTopicRouter Single Topic")
    func testRoundRobinSingleTopic() async {
        let router = RoundRobinTopicRouter<String>()
        let topics = ["single-topic"]
        let metadata = MessageMetadata()
        
        // Should always return the single topic
        let selected1 = await router.selectTopic(for: "message1", metadata: metadata, topics: topics)
        let selected2 = await router.selectTopic(for: "message2", metadata: metadata, topics: topics)
        
        #expect(selected1 == "single-topic")
        #expect(selected2 == "single-topic")
    }
    
    @Test("RoundRobinTopicRouter Empty Topics")
    func testRoundRobinEmptyTopics() async {
        let router = RoundRobinTopicRouter<String>()
        let topics: [String] = []
        let metadata = MessageMetadata()
        
        // Should return empty string for empty topics
        let selected = await router.selectTopic(for: "message", metadata: metadata, topics: topics)
        
        #expect(selected == "")
    }
    
    @Test("Topic Router Protocol Conformance")
    func testTopicRouterProtocol() async {
        let router = MockTopicRouter<String>()
        let topics = ["topicA", "topicB"]
        let metadata = MessageMetadata()
        
        // Verify router conforms to protocol and cycles through topics
        let selected1 = await router.selectTopic(for: "msg1", metadata: metadata, topics: topics)
        let selected2 = await router.selectTopic(for: "msg2", metadata: metadata, topics: topics)
        let selected3 = await router.selectTopic(for: "msg3", metadata: metadata, topics: topics)
        
        #expect(selected1 == "topicA")
        #expect(selected2 == "topicB")
        #expect(selected3 == "topicA") // Should cycle back
    }
}