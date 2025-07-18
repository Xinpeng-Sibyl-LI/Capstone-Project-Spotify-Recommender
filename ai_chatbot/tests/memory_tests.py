# ai_chatbot/tests/memory_tests.py
"""
Tests for conversation memory functionality
"""

import unittest
import os
import shutil
from ..utils.memory_utils import ConversationMemory
from langchain_core.messages import HumanMessage, AIMessage

class TestConversationMemory(unittest.TestCase):
    """Test cases for ConversationMemory"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_memory_dir = "./test_memory"
        self.memory = ConversationMemory(max_messages=5)
        self.memory.memory_file = f"{self.test_memory_dir}/test_conversation.pkl"
    
    def tearDown(self):
        """Clean up test environment"""
        if os.path.exists(self.test_memory_dir):
            shutil.rmtree(self.test_memory_dir)
    
    def test_add_messages(self):
        """Test adding messages to memory"""
        # Add user message
        self.memory.add_user_message("Hello, chatbot!")
        self.assertEqual(len(self.memory.messages), 1)
        self.assertIsInstance(self.memory.messages[0], HumanMessage)
        
        # Add AI message
        self.memory.add_ai_message("Hello! How can I help you?")
        self.assertEqual(len(self.memory.messages), 2)
        self.assertIsInstance(self.memory.messages[1], AIMessage)
    
    def test_message_limit(self):
        """Test that memory respects max message limit"""
        # Add more messages than the limit
        for i in range(10):
            self.memory.add_user_message(f"Message {i}")
        
        # Should only keep last 5 messages
        self.assertEqual(len(self.memory.messages), 5)
        self.assertEqual(self.memory.messages[0].content, "Message 5")
        self.assertEqual(self.memory.messages[-1].content, "Message 9")
    
    def test_clear_memory(self):
        """Test clearing memory"""
        # Add some messages
        self.memory.add_user_message("Test message 1")
        self.memory.add_ai_message("Response 1")
        
        # Clear memory
        self.memory.clear()
        
        # Check memory is empty
        self.assertEqual(len(self.memory.messages), 0)
        self.assertEqual(len(self.memory.message_embeddings), 0)
    
    def test_search_similar_conversations(self):
        """Test semantic search in conversation history"""
        # Add some messages about specific topics
        self.memory.add_user_message("What are the top artists by popularity?")
        self.memory.add_ai_message("The top artists are Bruno Mars, The Weeknd...")
        self.memory.add_user_message("Tell me about track engagement metrics")
        self.memory.add_ai_message("Track engagement is measured by skip rate...")
        
        # Search for similar content
        results = self.memory.search_similar_conversations("artists popularity", k=2)
        
        # Should find relevant messages
        self.assertIn("top artists", results.lower())
    
    def test_conversation_summary(self):
        """Test getting conversation summary"""
        # Add some messages
        self.memory.add_user_message("Hello!")
        self.memory.add_ai_message("Hi there!")
        self.memory.add_user_message("Show me some data")
        
        # Get summary
        summary = self.memory.get_summary()
        
        # Check summary contains expected info
        self.assertIn("3 messages", summary)
        self.assertIn("User messages: 2", summary)
        self.assertIn("AI responses: 1", summary)
    
    def test_persistence(self):
        """Test saving and loading memory"""
        # Add messages
        self.memory.add_user_message("Test persistence")
        self.memory.add_ai_message("Message saved")
        
        # Create new memory instance
        new_memory = ConversationMemory()
        new_memory.memory_file = self.memory.memory_file
        new_memory._load_memory()
        
        # Check messages were loaded
        self.assertEqual(len(new_memory.messages), 2)
        self.assertEqual(new_memory.messages[0].content, "Test persistence")

if __name__ == '__main__':
    unittest.main()