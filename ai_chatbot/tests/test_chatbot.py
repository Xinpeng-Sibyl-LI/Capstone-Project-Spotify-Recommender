#!/usr/bin/env python3
"""
Test script to verify all chatbot components are working
"""

from ..chatbot import SpotifyAnalyticsChatbot  # Changed import
from ..utils import test_connection  # Changed import
from ..tools import get_all_tools  # Changed import
import logging

logging.basicConfig(level=logging.INFO)

def run_tests():
    """Run tests for all components"""
    print("🧪 Running Chatbot Component Tests...\n")
    
    # Test 1: Snowflake Connection
    print("1️⃣ Testing Snowflake Connection...")
    if test_connection():
        print("   ✅ Snowflake connection successful")
    else:
        print("   ❌ Snowflake connection failed")
        return False
    
    # Test 2: Tool Registration
    print("\n2️⃣ Testing Tool Registration...")
    tools = get_all_tools()
    print(f"   ✅ Registered {len(tools)} tools:")
    for tool in tools:
        print(f"      - {tool.name}")
    
    # Test 3: Chatbot Initialization
    print("\n3️⃣ Testing Chatbot Initialization...")
    try:
        chatbot = SpotifyAnalyticsChatbot()
        print("   ✅ Chatbot initialized successfully")
    except Exception as e:
        print(f"   ❌ Chatbot initialization failed: {e}")
        return False
    
    # Test 4: Sample Queries
    print("\n4️⃣ Testing Sample Queries...")
    test_queries = [
        "Show me the batch data sources",
        "What's in the real-time data?",
        "When was the data last updated?"
    ]
    
    for query in test_queries:
        print(f"\n   Query: '{query}'")
        try:
            response = chatbot.chat(query)
            print(f"   ✅ Response received ({len(response)} chars)")
        except Exception as e:
            print(f"   ❌ Query failed: {e}")
    
    # Test 5: Memory
    print("\n5️⃣ Testing Conversation Memory...")
    summary = chatbot.get_conversation_summary()
    print(f"   ✅ Memory working - {len(chatbot.memory.messages)} messages stored")
    
    print("\n✨ All tests completed!")
    return True

if __name__ == "__main__":
    success = run_tests()
    import sys
    sys.exit(0 if success else 1)