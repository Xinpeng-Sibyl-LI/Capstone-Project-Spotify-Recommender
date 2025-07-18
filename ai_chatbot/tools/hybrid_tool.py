import openai
import os
import sys
from dotenv import load_dotenv

# Add project root to path
current_dir = os.path.dirname(__file__)  # ai_chatbot/tools/
parent_dir = os.path.dirname(current_dir)  # ai_chatbot/
project_root = os.path.dirname(parent_dir)  # project root
sys.path.append(project_root)
sys.path.append(current_dir)  # Add current directory

# Import from same directory and other modules
try:
    from query_tool import query_with_rerank
    RAG_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ RAG not available: {e}")
    RAG_AVAILABLE = False

try:
    from sql_tool import query_snowflake
    SQL_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ SQL tool not available: {e}")
    SQL_AVAILABLE = False

load_dotenv()

# Set up OpenAI client
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def classify_question(question: str) -> str:
    """Use GPT to classify the type of question for Spotify data."""
    prompt = f"""
You are a smart assistant for a Spotify music data analysis system. Classify this question:
"{question}"

Does it require:
- 'doc' → if the answer is in documentation (PDF/manual about music/Spotify, general knowledge about Spotify)
- 'sql' → if the answer needs data from the Spotify warehouse (artists, tracks, listening history, popularity, genres, play counts, etc.)
- 'both' → if it needs both sources

Examples:
- "What are my top 5 most played artists?" → sql
- "How many tracks are in the database?" → sql  
- "What genres are most popular?" → sql
- "Show me listening patterns by device" → sql
- "Who founded Spotify?" → doc
- "How does Spotify calculate popularity?" → doc
- "What languages are detected in my music?" → sql
- "Explain the Spotify API documentation" → doc

Only return one word: doc, sql, or both.
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        return response.choices[0].message.content.strip().lower()
    except Exception as e:
        print(f"⚠️ OpenAI classification error: {e}")
        # Default to SQL for most music data questions
        return "sql"

def simple_doc_response(question: str) -> str:
    """Simple documentation response without RAG."""
    return f"""📘 RAG functionality not fully set up. 
    
For questions about Spotify concepts like:
- Who founded Spotify
- How Spotify calculates popularity scores
- Music analysis concepts
- API documentation

Your question: "{question}"

To enable full document search:
1. Set up Pinecone API key in .env
2. Run: python ai_chatbot/tools/rag_tool.py
3. Then ask documentation questions again
"""

def query_hybrid(question: str) -> str:
    """Route the question to the correct tool based on GPT classification."""
    mode = classify_question(question)

    if mode == "doc":
        if RAG_AVAILABLE:
            try:
                doc_answer = query_with_rerank(question)
                return f"📘 From Documentation:\n{doc_answer}"
            except Exception as e:
                return f"📘 Documentation Error: {str(e)}\n\nFallback: {simple_doc_response(question)}"
        else:
            return simple_doc_response(question)

    elif mode == "sql":
        if SQL_AVAILABLE:
            try:
                sql_answer = query_snowflake(question)
                return f"📊 From Spotify Data:\n{sql_answer}"
            except Exception as e:
                return f"📊 Database Error: {str(e)}"
        else:
            return "📊 SQL functionality not available"

    elif mode == "both":
        responses = []
        
        # Try documentation
        if RAG_AVAILABLE:
            try:
                doc_answer = query_with_rerank(question)
                responses.append(f"📘 From Documentation:\n{doc_answer}")
            except Exception as e:
                responses.append(f"📘 Documentation Error: {str(e)}")
        else:
            responses.append(simple_doc_response(question))
        
        # Try SQL
        if SQL_AVAILABLE:
            try:
                sql_answer = query_snowflake(question)
                responses.append(f"📊 From Spotify Data:\n{sql_answer}")
            except Exception as e:
                responses.append(f"📊 Database Error: {str(e)}")
        
        return "\n\n".join(responses)

    else:
        return f"❓ Unable to classify question. GPT returned: '{mode}'"

# CLI test
if __name__ == "__main__":
    print("🧠 Hybrid Tool Test")
    print(f"RAG Available: {RAG_AVAILABLE}")
    print(f"SQL Available: {SQL_AVAILABLE}")
    
    q = input("🧠 Ask about your Spotify data: ")
    print("\n🔎 Answer:\n")
    print(query_hybrid(q))