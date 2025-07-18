# ai_chatbot/utils/memory_utils.py
import os
import json
import uuid
from datetime import datetime
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

# Where to save memory files
MEMORY_DIR = "ai_chatbot/memory"
os.makedirs(MEMORY_DIR, exist_ok=True)

def create_thread_id() -> str:
    """Generate a new unique thread ID."""
    return str(uuid.uuid4())

def get_memory_path(thread_id: str) -> str:
    """Get the file path for a specific thread."""
    return os.path.join(MEMORY_DIR, f"{thread_id}.json")

def save_message(thread_id: str, role: str, message: str) -> None:
    """Save a message under a thread."""
    try:
        memory = load_memory(thread_id)
        memory.append({
            "timestamp": datetime.utcnow().isoformat(),
            "role": role,
            "message": message
        })
        
        # Keep only last 50 messages to prevent file from growing too large
        if len(memory) > 50:
            memory = memory[-50:]
        
        with open(get_memory_path(thread_id), "w", encoding="utf-8") as f:
            json.dump(memory, f, indent=2, ensure_ascii=False)
            
        logger.debug(f"Saved message for thread {thread_id}: {role}")
        
    except Exception as e:
        logger.error(f"Error saving message: {e}")

def load_memory(thread_id: str) -> List[Dict[str, Any]]:
    """Load full message history for a thread."""
    try:
        path = get_memory_path(thread_id)
        if not os.path.exists(path):
            return []
        
        with open(path, "r", encoding="utf-8") as f:
            memory = json.load(f)
            
        # Ensure memory is a list
        if not isinstance(memory, list):
            logger.warning(f"Invalid memory format for thread {thread_id}")
            return []
        
        return memory
        
    except Exception as e:
        logger.error(f"Error loading memory for thread {thread_id}: {e}")
        return []

def clear_memory(thread_id: str) -> bool:
    """Delete memory file for a given thread."""
    try:
        path = get_memory_path(thread_id)
        if os.path.exists(path):
            os.remove(path)
            logger.info(f"Cleared memory for thread {thread_id}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error clearing memory: {e}")
        return False

def get_memory_summary(thread_id: str) -> Dict[str, Any]:
    """Get summary statistics for a thread's memory."""
    try:
        memory = load_memory(thread_id)
        
        if not memory:
            return {
                "total_messages": 0,
                "user_messages": 0,
                "assistant_messages": 0,
                "first_message": None,
                "last_message": None
            }
        
        user_messages = sum(1 for msg in memory if msg.get("role") == "user")
        assistant_messages = sum(1 for msg in memory if msg.get("role") == "assistant")
        
        return {
            "total_messages": len(memory),
            "user_messages": user_messages,
            "assistant_messages": assistant_messages,
            "first_message": memory[0].get("timestamp") if memory else None,
            "last_message": memory[-1].get("timestamp") if memory else None
        }
        
    except Exception as e:
        logger.error(f"Error getting memory summary: {e}")
        return {"error": str(e)}

def search_memory(thread_id: str, query: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Search through conversation history for relevant messages."""
    try:
        memory = load_memory(thread_id)
        
        if not memory:
            return []
        
        # Simple text search (could be enhanced with embeddings)
        query_lower = query.lower()
        relevant_messages = []
        
        for msg in memory:
            if query_lower in msg.get("message", "").lower():
                relevant_messages.append(msg)
        
        # Return most recent matches first
        return relevant_messages[-limit:]
        
    except Exception as e:
        logger.error(f"Error searching memory: {e}")
        return []

def cleanup_old_threads(days_old: int = 30) -> int:
    """Remove conversation threads older than specified days."""
    try:
        if not os.path.exists(MEMORY_DIR):
            return 0
        
        cutoff_time = datetime.utcnow().timestamp() - (days_old * 24 * 60 * 60)
        cleaned_count = 0
        
        for filename in os.listdir(MEMORY_DIR):
            if filename.endswith(".json"):
                file_path = os.path.join(MEMORY_DIR, filename)
                
                # Check file modification time
                if os.path.getmtime(file_path) < cutoff_time:
                    try:
                        os.remove(file_path)
                        cleaned_count += 1
                        logger.info(f"Removed old thread: {filename}")
                    except Exception as e:
                        logger.error(f"Error removing {filename}: {e}")
        
        return cleaned_count
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return 0

# Utility functions for thread management
def list_all_threads() -> List[str]:
    """List all active thread IDs."""
    try:
        if not os.path.exists(MEMORY_DIR):
            return []
        
        threads = []
        for filename in os.listdir(MEMORY_DIR):
            if filename.endswith(".json"):
                thread_id = filename[:-5]  # Remove .json extension
                threads.append(thread_id)
        
        return threads
        
    except Exception as e:
        logger.error(f"Error listing threads: {e}")
        return []

def export_conversation(thread_id: str, format: str = "json") -> str:
    """Export conversation in specified format."""
    try:
        memory = load_memory(thread_id)
        
        if format.lower() == "json":
            return json.dumps(memory, indent=2, ensure_ascii=False)
        
        elif format.lower() == "text":
            lines = []
            for msg in memory:
                timestamp = msg.get("timestamp", "Unknown")
                role = msg.get("role", "Unknown")
                message = msg.get("message", "")
                lines.append(f"[{timestamp}] {role.title()}: {message}")
            return "\n".join(lines)
        
        else:
            raise ValueError(f"Unsupported format: {format}")
            
    except Exception as e:
        logger.error(f"Error exporting conversation: {e}")
        return f"Error: {e}"