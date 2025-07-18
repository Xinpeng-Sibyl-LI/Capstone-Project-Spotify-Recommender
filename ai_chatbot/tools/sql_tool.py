# ai_chatbot/tools/sql_tool.py
import openai
from dotenv import load_dotenv
import os
import sys
import logging

# Add the project root to the path
current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(parent_dir)
sys.path.append(project_root)

try:
    from ai_chatbot.utils.snowflake_utils import execute_query
except ImportError:
    sys.path.append(os.path.join(parent_dir, 'utils'))
    from snowflake_utils import execute_query

load_dotenv()
logger = logging.getLogger(__name__)

# Set up OpenAI client
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def get_schema_context():
    """Get schema information for actual MARTS tables from Snowflake."""
    return """
Available Spotify Tables in RAW Schema:

1. MART_ARTIST_SUMMARY (in RAW schema)
   - ARTIST_FOLLOWERS (Number): Number of followers
   - ARTIST_ID (Varchar): Spotify artist ID
   - ARTIST_NAME (Varchar): Artist name
   - ARTIST_POPULARITY (Number): Artist popularity score (0-100)
   - ARTIST_TIER (Varchar): Artist tier classification
   - AVG_TRACK_POPULARITY (Number): Average popularity of artist's tracks
   - CONSISTENCY_RATING (Varchar): Consistency rating
   - MAX_TRACK_POPULARITY (Number): Highest track popularity
   - MIN_TRACK_POPULARITY (Number): Lowest track popularity
   - NUM_TRACKS (Number): Number of tracks by artist
   - POPULARITY_RANGE (Number): Range of track popularity
   - TRACK_POPULARITY_STD (Float): Standard deviation of track popularity

2. MART_TOP_TRACKS (in RAW schema)
   - ARTIST_FOLLOWERS (Number): Artist's follower count
   - ARTIST_ID (Varchar): Spotify artist ID
   - ARTIST_NAME (Varchar): Artist name
   - ARTIST_POPULARITY (Number): Artist popularity score
   - POPULARITY_RANGE (Varchar): Popularity range category
   - POPULARITY_RANK (Number): Rank by popularity
   - TRACK_ID (Varchar): Spotify track ID
   - TRACK_NAME (Varchar): Track name
   - TRACK_POPULARITY (Number): Track popularity score

3. MART_USER_DAILY_PLAYS (in RAW schema)
   - APPEAL_CATEGORY (Varchar): Appeal category
   - ARTIST_ID (Varchar): Spotify artist ID
   - ARTIST_NAME (Varchar): Artist name
   - ARTIST_POPULARITY (Number): Artist popularity score
   - AVG_LISTEN_DURATION (Number): Average listen duration
   - AVG_PLAYS_PER_LISTENER (Float): Average plays per listener
   - COMPLETION_RATE_PERCENT (Number): Completion rate percentage
   - DAYS_PLAYED (Number): Number of days played
   - DURATION_SECONDS (Number): Duration in seconds
   - ENGAGEMENT_TIER (Varchar): Engagement tier
   - IS_EXPLICIT (Boolean): Whether track is explicit
   - LANGUAGE_POPULARITY_RANK (Number): Language popularity rank
   - POPULARITY_RANGE (Varchar): Popularity range category
   - SKIP_RATE_PERCENT (Float): Skip rate percentage
   - TOTAL_PLAYS (Number): Total number of plays
   - TOTAL_SKIPS (Number): Total number of skips
   - TRACK_ID (Varchar): Spotify track ID
   - TRACK_LANGUAGE (Varchar): Track language
   - TRACK_NAME (Varchar): Track name
   - TRACK_POPULARITY (Number): Track popularity score
   - UNIQUE_LISTENERS (Number): Number of unique listeners

Fallback tables (basic data):
- RAW_TOP_ARTISTS: Basic artist info
- RAW_TOP_TRACKS: Basic track info  
- RAW_LISTENING_HISTORY: Basic listening data

Important Notes:
- Use table names without RAW prefix: MART_ARTIST_SUMMARY, MART_TOP_TRACKS, MART_USER_DAILY_PLAYS
- MART tables contain clean, aggregated data perfect for analysis
- Use ARTIST_NAME and TRACK_NAME for searches (case-insensitive with ILIKE)
- Join tables using ARTIST_ID and TRACK_ID
- All popularity scores are 0-100 scale
"""

def translate_question_to_sql(question: str) -> str:
    """Uses GPT to convert a natural language question to SQL for Spotify MARTS data."""
    schema_info = get_schema_context()
    
    prompt = f"""
You are an expert SQL writer for Spotify music analytics using clean MARTS data.

{schema_info}

Convert this question into a valid Snowflake SQL query:
"{question}"

Rules:
1. Return a valid SQL query that best answers the user's question
2. Use full table names: DBT_SPOTIFY.RAW.MART_ARTIST_SUMMARY, DBT_SPOTIFY.RAW.MART_TOP_TRACKS, DBT_SPOTIFY.RAW.MART_USER_DAILY_PLAYS
3. Use ILIKE for case-insensitive text searches on names
4. Interpret user questions flexibly - they won't use exact column names
5. For artist questions, use DBT_SPOTIFY.RAW.MART_ARTIST_SUMMARY for comprehensive info
6. For track questions, use DBT_SPOTIFY.RAW.MART_TOP_TRACKS or DBT_SPOTIFY.RAW.MART_USER_DAILY_PLAYS
7. For engagement/listening questions, use DBT_SPOTIFY.RAW.MART_USER_DAILY_PLAYS
8. Join tables when needed to provide complete answers
9. Limit results to 10 unless user asks for more
10. Use meaningful column aliases that make sense to users

Example question interpretations:
- "tell me about Taylor Swift" → SELECT * FROM DBT_SPOTIFY.RAW.MART_ARTIST_SUMMARY WHERE ARTIST_NAME ILIKE '%taylor swift%'
- "what are popular songs" → SELECT TRACK_NAME, ARTIST_NAME, TRACK_POPULARITY FROM DBT_SPOTIFY.RAW.MART_TOP_TRACKS ORDER BY TRACK_POPULARITY DESC LIMIT 10
- "show me engagement data" → SELECT TRACK_NAME, TOTAL_PLAYS, SKIP_RATE_PERCENT, COMPLETION_RATE_PERCENT FROM DBT_SPOTIFY.RAW.MART_USER_DAILY_PLAYS ORDER BY TOTAL_PLAYS DESC LIMIT 10
- "which artists have most followers" → SELECT ARTIST_NAME, ARTIST_FOLLOWERS FROM DBT_SPOTIFY.RAW.MART_ARTIST_SUMMARY ORDER BY ARTIST_FOLLOWERS DESC LIMIT 10
- "what languages do I listen to" → SELECT TRACK_LANGUAGE, COUNT(*) as track_count FROM DBT_SPOTIFY.RAW.MART_USER_DAILY_PLAYS GROUP BY TRACK_LANGUAGE ORDER BY track_count DESC
- "songs I skip a lot" → SELECT TRACK_NAME, SKIP_RATE_PERCENT FROM DBT_SPOTIFY.RAW.MART_USER_DAILY_PLAYS WHERE SKIP_RATE_PERCENT > 30 ORDER BY SKIP_RATE_PERCENT DESC
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=500
        )
        sql_query = response.choices[0].message.content.strip()
        
        # Clean up the SQL query
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        
        return sql_query
        
    except Exception as e:
        logger.error(f"OpenAI error: {e}")
        return f"SELECT 'Error generating SQL query: {e}' as error_message"

def format_results_user_friendly(results, question: str) -> str:
    """Format query results in a user-friendly, conversational way"""
    if not results:
        return "✅ I searched your data but didn't find any results for that question."
    
    try:
        # Handle single value results
        if len(results) == 1 and len(results[0]) == 1:
            value = list(results[0].values())[0]
            return f"📊 **Answer**: {value:,}" if isinstance(value, (int, float)) else f"📊 **Answer**: {value}"
        
        # Multiple results - format conversationally
        formatted_response = []
        first_row = results[0]
        columns = list(first_row.keys())
        
        # Artist summary information
        if 'ARTIST_NAME' in columns and 'ARTIST_FOLLOWERS' in columns:
            formatted_response.append("🎤 **Artist Information:**")
            for i, row in enumerate(results[:10], 1):
                name = row.get('ARTIST_NAME', 'Unknown')
                followers = row.get('ARTIST_FOLLOWERS', 0)
                popularity = row.get('ARTIST_POPULARITY', 0)
                num_tracks = row.get('NUM_TRACKS', 0)
                
                line = f"{i}. **{name}**"
                if followers > 0:
                    line += f" - {followers:,} followers"
                if popularity > 0:
                    line += f" (popularity: {popularity}/100)"
                if num_tracks > 0:
                    line += f" - {num_tracks} tracks"
                    
                formatted_response.append(line)
        
        # Track information
        elif 'TRACK_NAME' in columns and 'TRACK_POPULARITY' in columns:
            formatted_response.append("🎵 **Track Information:**")
            for i, row in enumerate(results[:10], 1):
                track_name = row.get('TRACK_NAME', 'Unknown')
                artist_name = row.get('ARTIST_NAME', 'Unknown Artist')
                popularity = row.get('TRACK_POPULARITY', 0)
                
                line = f"{i}. **{track_name}** by {artist_name}"
                if popularity > 0:
                    line += f" (popularity: {popularity}/100)"
                    
                formatted_response.append(line)
        
        # Engagement/listening data
        elif 'TOTAL_PLAYS' in columns or 'SKIP_RATE_PERCENT' in columns:
            formatted_response.append("📊 **Listening Analytics:**")
            for i, row in enumerate(results[:10], 1):
                track_name = row.get('TRACK_NAME', 'Unknown')
                total_plays = row.get('TOTAL_PLAYS', 0)
                skip_rate = row.get('SKIP_RATE_PERCENT', 0)
                completion_rate = row.get('COMPLETION_RATE_PERCENT', 0)
                
                line = f"{i}. **{track_name}**"
                if total_plays > 0:
                    line += f" - {total_plays:,} plays"
                if skip_rate > 0:
                    line += f" (skip rate: {skip_rate:.1f}%)"
                elif completion_rate > 0:
                    line += f" (completion rate: {completion_rate:.1f}%)"
                    
                formatted_response.append(line)
        
        # Language or category statistics
        elif len(columns) == 2 and any('count' in col.lower() for col in columns):
            formatted_response.append("📊 **Statistics:**")
            for row in results[:10]:
                keys = [k for k in row.keys() if 'count' not in k.lower()]
                counts = [v for k, v in row.items() if 'count' in k.lower()]
                
                if keys and counts:
                    key_name = keys[0]
                    key_value = row[key_name]
                    count_value = counts[0]
                    formatted_response.append(f"• **{key_value}**: {count_value:,}")
        
        # Generic formatting for other cases
        else:
            formatted_response.append(f"📊 **Results** ({len(results)} found):")
            for i, row in enumerate(results[:10], 1):
                row_items = []
                for key, value in row.items():
                    if value is not None:
                        if isinstance(value, (int, float)) and value > 1000:
                            row_items.append(f"{key}: {value:,}")
                        elif isinstance(value, float) and 0 < value < 1:
                            row_items.append(f"{key}: {value:.2f}")
                        else:
                            row_items.append(f"{key}: {value}")
                formatted_response.append(f"{i}. {' | '.join(row_items)}")
        
        # Add truncation notice
        if len(results) > 10:
            formatted_response.append(f"\n_... and {len(results) - 10} more results_")
        
        return "\n".join(formatted_response)
        
    except Exception as e:
        logger.error(f"Error formatting results: {e}")
        return f"📊 **Results**: Found {len(results)} results, but had trouble formatting them. Raw data: {results[:3]}..."

def query_snowflake(question: str) -> str:
    """Main function: translate question to SQL, execute it, and format results conversationally"""
    try:
        # Generate SQL query
        sql_query = translate_question_to_sql(question)
        
        if "error_message" in sql_query.lower():
            return sql_query
        
        logger.info(f"Generated SQL: {sql_query}")
        
        # Execute query
        results = execute_query(sql_query)
        
        # Format results in a user-friendly way
        formatted_result = format_results_user_friendly(results, question)
        
        return formatted_result
        
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        return f"❌ I encountered an error while analyzing your data: {str(e)}"

# CLI test function
def test_sql_tool():
    """Test the SQL tool with sample questions using MARTS data"""
    test_questions = [
        "How many artists do I have?",
        "What can you tell me about Taylor Swift?",
        "Show me the top 5 artists by followers",
        "What are the most popular tracks?",
        "Show me tracks with high skip rates",
        "Which languages are most common in my music?",
        "Tell me about engagement metrics"
    ]
    
    print("🧪 Testing SQL Tool with MARTS data...")
    for question in test_questions:
        print(f"\n❓ Question: {question}")
        result = query_snowflake(question)
        print(f"🤖 Answer: {result}")
        print("-" * 60)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_sql_tool()
    else:
        question = input("❓ Ask a question about your Spotify data: ")
        print("\n🤖 Answer:")
        print(query_snowflake(question))