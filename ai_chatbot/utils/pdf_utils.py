import os
from PyPDF2 import PdfReader

def load_pdf_text(filepath):
    """Loads and concatenates all text from a PDF file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"PDF not found at {filepath}")
    
    reader = PdfReader(filepath)
    full_text = ""

    for page in reader.pages:
        text = page.extract_text()
        if text:
            cleaned = " ".join(text.strip().splitlines())
            full_text += cleaned + "\n"

    return full_text.strip()

def split_into_chunks(text, max_tokens=500):
    """
    Splits text into paragraph-based chunks for embedding.
    Each chunk tries to stay within the token budget.
    """
    try:
        import tiktoken  # requires OpenAI tokenizer for accuracy
        encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
    except ImportError:
        # Fallback: estimate tokens (roughly 4 characters per token)
        def count_tokens(text):
            return len(text) // 4
        
        chunks = []
        current_chunk = ""
        
        for paragraph in text.split("\n\n"):
            paragraph = paragraph.strip()
            if not paragraph:
                continue
            
            current_tokens = count_tokens(current_chunk)
            paragraph_tokens = count_tokens(paragraph)
            
            if current_tokens + paragraph_tokens < max_tokens:
                current_chunk += paragraph + "\n\n"
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = paragraph + "\n\n"
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks
    
    # Use tiktoken if available
    chunks = []
    current_chunk = ""

    for paragraph in text.split("\n\n"):
        paragraph = paragraph.strip()
        if not paragraph:
            continue

        current_tokens = len(encoding.encode(current_chunk))
        paragraph_tokens = len(encoding.encode(paragraph))

        if current_tokens + paragraph_tokens < max_tokens:
            current_chunk += paragraph + "\n\n"
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = paragraph + "\n\n"

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks

# Alternative function names for compatibility
def load_pdf(filepath):
    """Alias for load_pdf_text."""
    return load_pdf_text(filepath)

def chunk_text(text, max_tokens=500):
    """Alias for split_into_chunks."""
    return split_into_chunks(text, max_tokens)