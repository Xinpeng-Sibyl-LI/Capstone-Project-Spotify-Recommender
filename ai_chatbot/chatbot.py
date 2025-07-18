# ai_chatbot/chatbot.py
import sys
import os
import logging
import importlib.util

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fix paths properly using absolute paths
current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file)
project_root = os.path.dirname(current_dir)

# Add to Python path
sys.path.insert(0, project_root)

print(f"🔧 Debug paths:")
print(f"  Current file: {current_file}")
print(f"  Current dir: {current_dir}")
print(f"  Project root: {project_root}")

# Direct import of hybrid_tool to use all functionality
def load_hybrid_tool():
    """Load hybrid_tool directly from file"""
    try:
        hybrid_tool_path = os.path.join(current_dir, 'tools', 'hybrid_tool.py')
        
        if os.path.exists(hybrid_tool_path):
            spec = importlib.util.spec_from_file_location("hybrid_tool", hybrid_tool_path)
            hybrid_tool = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(hybrid_tool)
            
            if hasattr(hybrid_tool, 'query_hybrid'):
                print("✅ Loaded hybrid_tool successfully")
                return hybrid_tool.query_hybrid
        
        print("❌ hybrid_tool not found or doesn't have query_hybrid function")
        return None
        
    except Exception as e:
        print(f"❌ Error loading hybrid_tool: {e}")
        return None

# Load memory utilities
try:
    from ai_chatbot.utils import memory_utils as mem
    print("✅ Loaded memory utilities")
except ImportError:
    print("⚠️ Memory utilities not available, creating minimal version")
    import uuid
    
    class SimpleMemory:
        def create_thread_id(self):
            return str(uuid.uuid4())
        def save_message(self, thread_id, role, message):
            pass
        def load_memory(self, thread_id):
            return []
        def clear_memory(self, thread_id):
            pass
    
    mem = SimpleMemory()

class SpotifyAnalyticsChatbot:
    """Main chatbot class that uses hybrid tool for SQL + RAG"""
    
    def __init__(self):
        self.thread_id = mem.create_thread_id()
        self.session_messages = 0
        self.query_hybrid = load_hybrid_tool()
        
        print(f"🤖 Chatbot initialized")
        print(f"  Thread ID: {self.thread_id[:8]}...")
        print(f"  Hybrid tool available: {self.query_hybrid is not None}")
    
    def chat(self, user_input: str) -> str:
        """Process user input using hybrid tool (SQL + RAG + General)"""
        if not user_input.strip():
            return "Please ask me something!"
        
        # Save user message
        mem.save_message(self.thread_id, "user", user_input)
        self.session_messages += 1
        
        try:
            if self.query_hybrid:
                # Use hybrid tool for classification and routing
                response = self.query_hybrid(user_input)
            else:
                response = """❌ Hybrid tool not available. 
                
This means I can't classify your question properly to decide between:
- SQL queries for your data
- RAG search through your PDF documents
- General conversation

Please check that hybrid_tool.py is working properly."""
            
            # Save assistant response
            mem.save_message(self.thread_id, "assistant", response)
            return response
            
        except Exception as e:
            logger.error(f"Error processing chat: {e}")
            error_response = f"❌ Sorry, I encountered an error: {str(e)}"
            mem.save_message(self.thread_id, "assistant", error_response)
            return error_response
    
    def get_conversation_summary(self) -> str:
        """Get summary of current conversation"""
        memory = mem.load_memory(self.thread_id)
        
        if not memory:
            return "No conversation history yet."
        
        user_messages = sum(1 for msg in memory if msg.get("role") == "user")
        assistant_messages = sum(1 for msg in memory if msg.get("role") == "assistant")
        
        return f"""📊 **Conversation Summary**
Thread ID: {self.thread_id}
Total messages: {len(memory)}
User messages: {user_messages}
Assistant responses: {assistant_messages}
Session messages: {self.session_messages}
Hybrid tool available: {self.query_hybrid is not None}"""

def print_welcome():
    """Print welcome message"""
    print("🎵 Spotify Analytics Chatbot")
    print("="*50)
    print("I can help you with:")
    print("📊 **Data Analysis** - Ask about your Spotify data")
    print("📚 **Documentation** - Questions about Spotify concepts")
    print("💬 **General Chat** - Casual conversation")
    print()
    print("🎯 **Example Questions:**")
    print("  Data: 'How many artists do I have?'")
    print("  Data: 'What can you tell me about Taylor Swift?'")
    print("  Docs: 'How does Spotify calculate popularity?'")
    print("  General: 'What can you help me with?'")
    print()
    print("Type 'exit' to quit, 'help' for more info.")
    print("="*50)

def test_hybrid_availability():
    """Test if hybrid tool and its components work"""
    print("🔍 Testing system components...")
    
    # Test hybrid tool
    query_hybrid = load_hybrid_tool()
    if query_hybrid:
        print("✅ Hybrid tool: Available")
        
        # Test a simple classification
        try:
            result = query_hybrid("Hello")
            print("✅ Hybrid tool: Working (responded to test)")
        except Exception as e:
            print(f"❌ Hybrid tool: Error during test - {e}")
    else:
        print("❌ Hybrid tool: Not available")
    
    # Test if RAG components exist
    rag_tool_path = os.path.join(current_dir, 'tools', 'rag_tool.py')
    query_tool_path = os.path.join(current_dir, 'tools', 'query_tool.py')
    
    print(f"📁 RAG tool exists: {os.path.exists(rag_tool_path)}")
    print(f"📁 Query tool exists: {os.path.exists(query_tool_path)}")
    
    # Test PDF file
    pdf_path = os.path.join(current_dir, 'source', 'spotify_joke.pdf')
    print(f"📄 PDF file exists: {os.path.exists(pdf_path)}")
    
    return query_hybrid is not None

def main():
    """Main chatbot loop"""
    print_welcome()
    
    # Test system
    if not test_hybrid_availability():
        print("\n⚠️ Some components are missing. Functionality may be limited.")
    
    print("\n🤖 Chatbot ready! Ask me anything:")
    print()
    
    # Initialize chatbot
    try:
        chatbot = SpotifyAnalyticsChatbot()
    except Exception as e:
        print(f"❌ Failed to initialize chatbot: {e}")
        return
    
    # Main interaction loop
    while True:
        try:
            user_input = input("👤 You: ").strip()
            
            # Handle special commands
            if user_input.lower() in ["exit", "quit"]:
                print("👋 Thanks for using the Spotify Analytics Chatbot!")
                break
            
            elif user_input.lower() == "help":
                print("""
🆘 **Help:**
- **Data questions**: Ask about your Spotify artists, tracks, listening habits
- **Documentation**: Ask about Spotify concepts, API, music theory
- **General**: Chat naturally, ask what I can do
- **Commands**: 'summary' for stats, 'clear' to reset, 'exit' to quit
""")
                continue
            
            elif user_input.lower() == "summary":
                print(chatbot.get_conversation_summary())
                continue
            
            elif user_input.lower() == "clear":
                mem.clear_memory(chatbot.thread_id)
                print("🗑️ Conversation history cleared!")
                continue
            
            elif not user_input:
                continue
            
            # Process the question using hybrid tool
            print("🤖 Bot: ", end="")
            response = chatbot.chat(user_input)
            print(response)
            print()
            
        except KeyboardInterrupt:
            print("\n\n👋 Session interrupted. Goodbye!")
            break
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            logger.error(f"Main loop error: {e}")
            continue

if __name__ == "__main__":
    main()