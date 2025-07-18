# ai_chatbot/tools/hybrid_tool.py
import openai
import os
import sys
import logging
from dotenv import load_dotenv

# Setup paths
current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(parent_dir)
sys.path.append(project_root)

# Import SQL tool
try:
    from ai_chatbot.tools.sql_tool import query_snowflake
except ImportError:
    try:
        from ai_chatbot.tools.sql_tool import query_snowflake
    except ImportError:
        def query_snowflake(question):
            return "❌ SQL tool not available. Please check your imports."

load_dotenv()
logger = logging.getLogger(__name__)

# Set up OpenAI client
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def classify_question(question: str) -> str:
    """Use GPT to classify the type of question for Spotify data."""
    prompt = f"""
You are a smart assistant for a Spotify music data analysis system. Classify this question:
"{question}"

Classification options:
- 'sql' → Question needs data from Spotify database (artists, tracks, listening history, popularity, play counts, etc.)
- 'doc' → Question needs documentation about Spotify concepts, API, or music theory
- 'general' → General conversation, greeting, or unclear question

Examples:
- "What are my top 5 most played artists?" → sql
- "How many tracks are in the database?" → sql  
- "What genres are most popular?" → sql
- "Show me listening patterns by device" → sql
- "How does Spotify calculate popularity?" → doc
- "What languages are detected in my music?" → sql
- "Explain the Spotify API documentation" → doc
- "Hello, how are you?" → general
- "What can you help me with?" → general

Only return one word: sql, doc, or general
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=10
        )
        classification = response.choices[0].message.content.strip().lower()
        
        # Validate classification
        if classification not in ['sql', 'doc', 'general']:
            logger.warning(f"Invalid classification: {classification}, defaulting to 'sql'")
            return 'sql'
            
        return classification
        
    except Exception as e:
        logger.error(f"OpenAI classification error: {e}")
        return "sql"  # Default to SQL for data questions

def handle_general_question(question: str) -> str:
    """Handle general conversation and help questions"""
    help_prompts = ["help", "what can you do", "what are you", "how do you work"]
    
    if any(prompt in question.lower() for prompt in help_prompts):
        return """🎵 **Spotify Analytics Chatbot**

I can help you analyze your Spotify music data! Here's what I can do:

**📊 Data Analysis:**
- Find your top artists and tracks
- Analyze listening patterns by device, time, language
- Show popularity scores and follower counts
- Track skip rates and engagement metrics
- Explore genres and music preferences

**💡 Example Questions:**
- "What are my top 10 most played artists?"
- "How many tracks do I have in each language?"
- "Show me my listening history by device"
- "Which genres are most popular in my collection?"
- "What's my average track skip rate?"
- "Find tracks longer than 5 minutes"

**🔧 Available Data:**
- Top Artists (followers, popularity, genres)
- Top Tracks (duration, language, popularity)
- Listening History (play times, devices, skip rates)

Just ask me anything about your Spotify data!
"""
    
    # Handle greetings
    greetings = ["hello", "hi", "hey", "good morning", "good afternoon"]
    if any(greeting in question.lower() for greeting in greetings):
        return """👋 Hello! I'm your Spotify Analytics Assistant. 

I can help you explore and analyze your Spotify music data. Try asking me about your top artists, listening patterns, or any music-related questions!

Type 'help' to see what I can do for you."""
    
    # Default response for other general questions
    return f"""🤔 I'm not sure how to help with that specific question. 

I specialize in analyzing Spotify music data. You can ask me about:
- Your music listening patterns
- Artist and track information
- Popularity scores and statistics
- Device usage and preferences

Would you like to ask something about your Spotify data instead?"""

def handle_doc_question(question: str) -> str:
    """Handle documentation-related questions"""
    return f"""📚 **Documentation Query**

Your question: "{question}"

This would typically be answered using documentation about:
- Spotify API concepts
- Music theory and analysis
- Data collection methods
- Popularity calculation algorithms

**Quick Info:**
- **Popularity Score**: Spotify's 0-100 scale based on recent play counts
- **Language Detection**: Automatically detected from track/artist names
- **Genres**: Associated with artists, stored as JSON arrays
- **Explicit Content**: Flagged by Spotify for explicit lyrics

For detailed documentation, you might want to check:
- Spotify for Developers (developer.spotify.com)
- Spotify Web API documentation
- Music information retrieval resources

Would you like me to query your actual music data instead?"""

def query_hybrid(question: str) -> str:
    """Main function: classify question and route to appropriate handler"""
    try:
        # Classify the question
        classification = classify_question(question)
        logger.info(f"Question classified as: {classification}")
        
        if classification == "sql":
            # Handle data queries
            try:
                result = query_snowflake(question)
                return f"📊 **Data Analysis Result**\n\n{result}"
            except Exception as e:
                logger.error(f"SQL query error: {e}")
                return f"❌ Error querying data: {str(e)}"
        
        elif classification == "doc":
            # Handle documentation questions
            return handle_doc_question(question)
        
        elif classification == "general":
            # Handle general conversation
            return handle_general_question(question)
        
        else:
            return f"❓ I'm not sure how to categorize your question. Classification: {classification}"
            
    except Exception as e:
        logger.error(f"Hybrid query error: {e}")
        return f"❌ Error processing your question: {str(e)}"

# CLI test function
def test_hybrid_tool():
    """Test the hybrid tool with various question types"""
    test_questions = [
        "Hello, how are you?",
        "What can you help me with?",
        "What are my top 5 artists?",
        "How does Spotify calculate popularity?",
        "Show me tracks in English",
        "Help me understand what you can do"
    ]
    
    print("🧪 Testing Hybrid Tool...")
    for question in test_questions:
        print(f"\n❓ Question: {question}")
        result = query_hybrid(question)
        print(f"🤖 Answer: {result}")
        print("-" * 50)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_hybrid_tool()
    else:
        question = input("🧠 Ask about your Spotify data: ")
        print("\n🔎 Answer:")
        print(query_hybrid(question))