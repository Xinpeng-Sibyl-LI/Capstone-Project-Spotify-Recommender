import os
import uuid
import openai
import sys
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

# Set up OpenAI client
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def load_pdf_text(filepath):
    """Load text from PDF using PyPDF2."""
    try:
        from PyPDF2 import PdfReader
    except ImportError:
        print("❌ PyPDF2 not installed. Run: pip install PyPDF2")
        return ""
    
    if not os.path.exists(filepath):
        print(f"❌ PDF file not found: {filepath}")
        return ""
    
    try:
        reader = PdfReader(filepath)
        full_text = ""

        for page in reader.pages:
            text = page.extract_text()
            if text:
                cleaned = " ".join(text.strip().splitlines())
                full_text += cleaned + "\n"

        return full_text.strip()
    except Exception as e:
        print(f"❌ Error reading PDF: {e}")
        return ""

def split_into_chunks(text, max_chars=2000):
    """Split text into chunks (simple character-based splitting)."""
    if not text:
        return []
    
    chunks = []
    current_chunk = ""
    
    # Split by paragraphs first
    paragraphs = text.split("\n\n")
    
    for paragraph in paragraphs:
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        
        # If adding this paragraph would exceed max_chars, save current chunk
        if len(current_chunk) + len(paragraph) > max_chars and current_chunk:
            chunks.append(current_chunk.strip())
            current_chunk = paragraph + "\n\n"
        else:
            current_chunk += paragraph + "\n\n"
    
    # Add the last chunk
    if current_chunk:
        chunks.append(current_chunk.strip())
    
    return chunks

def embed_text(text: str):
    """Embed a text chunk using OpenAI."""
    try:
        response = client.embeddings.create(
            input=[text],
            model="text-embedding-ada-002"
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"❌ Error creating embedding: {e}")
        return None

def save_embeddings_locally(pdf_path, chunks, embeddings):
    """Save embeddings locally as a fallback if Pinecone fails."""
    import json
    
    # Create local storage directory
    storage_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "embeddings")
    os.makedirs(storage_dir, exist_ok=True)
    
    # Prepare data
    data = {
        "source": os.path.basename(pdf_path),
        "chunks": []
    }
    
    for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
        if embedding:
            data["chunks"].append({
                "id": str(uuid.uuid4()),
                "chunk_index": i,
                "text": chunk,
                "embedding": embedding
            })
    
    # Save to file
    storage_path = os.path.join(storage_dir, f"{os.path.basename(pdf_path)}.json")
    with open(storage_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    print(f"💾 Saved {len(data['chunks'])} embeddings locally to {storage_path}")
    return True

def ingest_spotify_documentation_local():
    """Ingest Spotify documentation using local storage (no Pinecone required)."""
    # Look for PDFs in the source folder
    current_dir = os.path.dirname(__file__)  # ai_chatbot/tools/
    source_dir = os.path.join(os.path.dirname(current_dir), "source")
    
    print(f"🔍 Looking for PDFs in: {source_dir}")
    
    if not os.path.exists(source_dir):
        print(f"❌ Source directory not found: {source_dir}")
        return False
    
    # Find all PDF files
    pdf_files = [f for f in os.listdir(source_dir) if f.lower().endswith('.pdf')]
    
    if not pdf_files:
        print(f"❌ No PDF files found in {source_dir}")
        return False
    
    print(f"📄 Found PDF files: {pdf_files}")
    
    success_count = 0
    for pdf_file in pdf_files:
        pdf_path = os.path.join(source_dir, pdf_file)
        print(f"\n🔄 Processing: {pdf_file}")
        
        # Load and chunk PDF
        text = load_pdf_text(pdf_path)
        if not text:
            continue
        
        chunks = split_into_chunks(text)
        print(f"📦 Created {len(chunks)} chunks from PDF")
        
        if not chunks:
            continue
        
        # Create embeddings
        embeddings = []
        for i, chunk in enumerate(chunks):
            print(f"🔄 Creating embedding {i+1}/{len(chunks)}")
            embedding = embed_text(chunk)
            embeddings.append(embedding)
        
        # Save locally
        if save_embeddings_locally(pdf_path, chunks, embeddings):
            success_count += 1
    
    print(f"\n✅ Successfully processed {success_count}/{len(pdf_files)} PDF files")
    return success_count > 0

def setup_pinecone_fallback():
    """Try to set up Pinecone, fall back to local storage if it fails."""
    api_key = os.getenv("PINECONE_API_KEY")
    if not api_key:
        print("⚠️ PINECONE_API_KEY not found, using local storage instead")
        return ingest_spotify_documentation_local()
    
    try:
        # Try the newer Pinecone import first
        from pinecone import Pinecone, ServerlessSpec
        
        pc = Pinecone(api_key=api_key)
        index_name = "spotify-music-doc-index"
        
        # Check if index exists, create if not
        existing_indexes = [index.name for index in pc.list_indexes()]
        
        if index_name not in existing_indexes:
            print(f"🔧 Creating Pinecone index: {index_name}")
            pc.create_index(
                name=index_name,
                dimension=1536,
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1")
            )
            print(f"✅ Created index: {index_name}")
        
        index = pc.Index(index_name)
        print("✅ Pinecone setup successful")
        return ingest_with_pinecone(index)
        
    except ImportError:
        print("⚠️ Pinecone not installed or incompatible version")
        return ingest_spotify_documentation_local()
    except Exception as e:
        print(f"⚠️ Pinecone setup failed: {e}")
        print("📦 Falling back to local storage...")
        return ingest_spotify_documentation_local()

def ingest_with_pinecone(index):
    """Ingest using Pinecone."""
    current_dir = os.path.dirname(__file__)
    source_dir = os.path.join(os.path.dirname(current_dir), "source")
    
    pdf_files = [f for f in os.listdir(source_dir) if f.lower().endswith('.pdf')]
    namespace = "spotify-docs"
    
    for pdf_file in pdf_files:
        pdf_path = os.path.join(source_dir, pdf_file)
        print(f"\n🔄 Processing: {pdf_file}")
        
        text = load_pdf_text(pdf_path)
        chunks = split_into_chunks(text)
        
        upserts = []
        for i, chunk in enumerate(chunks):
            embedding = embed_text(chunk)
            if embedding:
                upserts.append({
                    "id": str(uuid.uuid4()),
                    "values": embedding,
                    "metadata": {
                        "source": pdf_file,
                        "chunk_index": i,
                        "text": chunk
                    }
                })
        
        if upserts:
            index.upsert(vectors=upserts, namespace=namespace)
            print(f"✅ Uploaded {len(upserts)} chunks to Pinecone")
    
    return True

# CLI entry point
if __name__ == "__main__":
    print("📚 RAG Tool - Ingesting Spotify Documentation")
    print("=" * 50)
    
    # Check OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ OPENAI_API_KEY not found in environment variables")
        exit(1)
    
    # Try Pinecone, fall back to local storage
    success = setup_pinecone_fallback()
    
    if success:
        print("\n🎉 RAG setup complete! Your chatbot can now answer documentation questions.")
        print("💡 If using local storage, make sure to update your query_tool.py to read from local files.")
    else:
        print("\n❌ RAG setup failed. Check the errors above.")