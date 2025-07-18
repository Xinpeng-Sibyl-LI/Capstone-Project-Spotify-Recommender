import os
import openai
import sys
import json
import numpy as np
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up OpenAI client
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def embed_query(query: str):
    """Embed user query using OpenAI."""
    try:
        response = client.embeddings.create(
            input=[query],
            model="text-embedding-ada-002"
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"❌ Error creating query embedding: {e}")
        return None

def cosine_similarity(a, b):
    """Calculate cosine similarity between two vectors."""
    try:
        # Convert to numpy arrays
        a = np.array(a)
        b = np.array(b)
        
        # Calculate cosine similarity
        dot_product = np.dot(a, b)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        
        if norm_a == 0 or norm_b == 0:
            return 0
        
        return dot_product / (norm_a * norm_b)
    except Exception as e:
        print(f"❌ Error calculating similarity: {e}")
        return 0

def load_local_embeddings():
    """Load embeddings from local storage."""
    current_dir = os.path.dirname(__file__)  # ai_chatbot/tools/
    embeddings_dir = os.path.join(os.path.dirname(current_dir), "embeddings")
    
    if not os.path.exists(embeddings_dir):
        return []
    
    all_chunks = []
    
    # Load all embedding files
    for filename in os.listdir(embeddings_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(embeddings_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_chunks.extend(data.get('chunks', []))
            except Exception as e:
                print(f"⚠️ Error loading {filename}: {e}")
    
    return all_chunks

def search_similar_chunks_local(query: str, top_k=5):
    """Search for similar chunks using local embeddings."""
    # Get query embedding
    query_embedding = embed_query(query)
    if not query_embedding:
        return []
    
    # Load local embeddings
    chunks = load_local_embeddings()
    if not chunks:
        return []
    
    # Calculate similarities
    similarities = []
    for chunk in chunks:
        if 'embedding' in chunk:
            similarity = cosine_similarity(query_embedding, chunk['embedding'])
            chunk['similarity'] = similarity
            similarities.append(chunk)
    
    # Sort by similarity and return top_k
    similarities.sort(key=lambda x: x['similarity'], reverse=True)
    return similarities[:top_k]

def rerank_chunks_faithful(query: str, chunks: list):
    """Use GPT to answer based EXACTLY on the document content."""
    if not chunks:
        return "No relevant information found in documents."
    
    # Get text from chunks
    context_blocks = []
    for i, chunk in enumerate(chunks):
        text = chunk.get('text', '[No text available]')
        context_blocks.append(f"Document Chunk {i+1}:\n{text}")
    
    context_text = "\n\n".join(context_blocks)

    prompt = f"""
You are a document assistant. A user asked: "{query}"

Here is the EXACT content from the document:
{context_text}

IMPORTANT INSTRUCTIONS:
1. Answer based ONLY on what is written in the document above
2. Do NOT correct or add information from your general knowledge
3. If the document contains jokes, humor, or incorrect information, present it as written
4. Quote directly from the document when possible
5. If the document doesn't contain relevant information for the question, say so

Question: {query}
Answer based only on the document content:
"""

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,  # Lower temperature for more faithful responses
            max_tokens=500
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"❌ Error with GPT reranking: {e}")
        # Fallback: return raw document text
        if chunks:
            chunk = chunks[0]
            return f"Document content: {chunk.get('text', 'Error processing chunks')}"
        return "Error processing query"

def query_with_rerank(user_query: str):
    """Main function to query documents with faithful reranking."""
    # Always try local first
    chunks = search_similar_chunks_local(user_query)
    
    if not chunks:
        return """❌ No relevant documents found. 
        
Make sure you've ingested documents by running:
python ai_chatbot/tools/rag_tool.py
"""
    
    print(f"📊 Found {len(chunks)} relevant chunks")
    
    # Show similarity scores for debugging
    for i, chunk in enumerate(chunks):
        similarity = chunk.get('similarity', 0)
        print(f"  Chunk {i+1}: {similarity:.3f} similarity")
    
    answer = rerank_chunks_faithful(user_query, chunks)
    return answer

# CLI test
if __name__ == "__main__":
    print("🔍 Query Tool Test - Faithful to Document Content")
    
    # Check if we have any embeddings
    local_chunks = load_local_embeddings()
    print(f"📦 Local embeddings available: {len(local_chunks)} chunks")
    
    if not local_chunks:
        print("⚠️ No local embeddings found. Run: python ai_chatbot/tools/rag_tool.py")
        exit(1)
    
    # Show what's in the first chunk for debugging
    if local_chunks:
        first_chunk_text = local_chunks[0].get('text', 'No text')
        print(f"📄 First chunk preview: {first_chunk_text[:200]}...")
    
    user_q = input("\n❓ Ask something about the document content: ")
    response = query_with_rerank(user_q)
    print("\n🧠 RAG Answer (faithful to document):\n", response)