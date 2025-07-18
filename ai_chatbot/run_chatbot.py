#!/usr/bin/env python3
"""
CLI interface for the Spotify Analytics Chatbot
"""

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.markdown import Markdown
from rich.prompt import Prompt
from .chatbot_old import SpotifyAnalyticsChatbot  # Changed from 'from ai_chatbot import'
from .utils import test_connection  # Changed import
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='chatbot.log'
)

console = Console()

def show_demo_queries():
    """Display demo queries for testing"""
    table = Table(title="🎯 Demo Queries for Capstone Test", show_header=True)
    table.add_column("Category", style="cyan", width=20)
    table.add_column("Query", style="white")
    
    queries = [
        ("✅ Batch Data", "Show me the batch data sources and when they were last updated"),
        ("✅ Real-time Data", "What's happening in the real-time streaming data?"),
        ("✅ ML Model", "What are the ML model predictions for track popularity?"),
        ("✅ Data Freshness", "When was each data source last updated?"),
        ("✅ Custom SQL", "Execute query: SELECT COUNT(*) FROM MART_TOP_TRACKS"),
        ("✅ Document Search", "What does the PDF say about music popularity?"),
        ("✅ Hybrid Search", "Search for information about engagement metrics in both data and documents"),
    ]
    
    for category, query in queries:
        table.add_row(category, query)
    
    console.print(table)

def show_help():
    """Show help information"""
    help_text = """
    **Available Commands:**
    
    • `help` - Show this help message
    • `demo` - Show demo queries for testing
    • `pdf <path>` - Load a PDF document
    • `clear` - Clear conversation history
    • `summary` - Show conversation summary
    • `test` - Test Snowflake connection
    • `quit` or `exit` - Exit the chatbot
    
    **Example Usage:**
    1. Load a PDF: `pdf spotify_guide.pdf`
    2. Ask about data: "Show me the top artists"
    3. Combine sources: "How do the PDF insights compare to our data?"
    """
    console.print(Panel(Markdown(help_text), title="📚 Help", border_style="blue"))

def main():
    console.print(Panel.fit(
        "[bold blue]🎵 Spotify Analytics AI Chatbot[/bold blue]\n"
        "[yellow]Capstone Project - Ready for Demo![/yellow]\n\n"
        "Features:\n"
        "✅ Batch & Real-time Data Queries\n"
        "✅ ML Model Integration\n"
        "✅ PDF Document RAG\n"
        "✅ Conversation Memory\n"
        "✅ Custom SQL Execution\n\n"
        "Type 'help' for commands or 'demo' for test queries",
        border_style="blue"
    ))
    
    # Test Snowflake connection
    console.print("\n[yellow]Testing Snowflake connection...[/yellow]")
    if test_connection():
        console.print("[green]✅ Snowflake connection successful![/green]")
    else:
        console.print("[red]❌ Snowflake connection failed! Check your .env file[/red]")
        return
    
    # Initialize chatbot
    try:
        chatbot = SpotifyAnalyticsChatbot()
        console.print("[green]✅ Chatbot initialized successfully![/green]\n")
    except Exception as e:
        console.print(f"[red]❌ Failed to initialize chatbot: {e}[/red]")
        return
    
    # Main chat loop
    while True:
        try:
            # Get user input
            user_input = Prompt.ask("\n[bold green]You[/bold green]")
            
            # Handle commands
            if user_input.lower() in ['quit', 'exit']:
                console.print("[yellow]👋 Goodbye! Thanks for using Spotify Analytics Chatbot![/yellow]")
                break
            
            elif user_input.lower() == 'help':
                show_help()
                continue
            
            elif user_input.lower() == 'demo':
                show_demo_queries()
                continue
            
            elif user_input.lower() == 'clear':
                chatbot.clear_memory()
                console.print("[green]✅ Conversation history cleared![/green]")
                continue
            
            elif user_input.lower() == 'summary':
                summary = chatbot.get_conversation_summary()
                console.print(Panel(summary, title="📊 Conversation Summary", border_style="blue"))
                continue
            
            elif user_input.lower() == 'test':
                if test_connection():
                    console.print("[green]✅ Snowflake connection is working![/green]")
                else:
                    console.print("[red]❌ Snowflake connection failed![/red]")
                continue
            
            elif user_input.lower().startswith('pdf '):
                pdf_path = user_input[4:].strip()
                console.print(f"[yellow]📄 Processing PDF: {pdf_path}[/yellow]")
                result = chatbot.process_pdf(pdf_path)
                console.print(result)
                continue
            
            # Regular chat
            console.print("\n[bold blue]Assistant[/bold blue]: ", end="")
            with console.status("[yellow]Thinking...[/yellow]", spinner="dots"):
                response = chatbot.chat(user_input)
            
            # Display response with markdown formatting
            console.print(Markdown(response))
            
        except KeyboardInterrupt:
            console.print("\n[yellow]👋 Goodbye![/yellow]")
            break
        except Exception as e:
            console.print(f"\n[red]❌ Error: {str(e)}[/red]")
            logging.error(f"Chat error: {e}", exc_info=True)

if __name__ == "__main__":
    main()