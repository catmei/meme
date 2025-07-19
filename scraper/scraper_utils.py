import os
import time
import praw
from dotenv import load_dotenv

# --- Initialization ---

def initialize_praw():
    """
    Initializes and returns an authenticated PRAW Reddit instance.
    
    Reads credentials from a .env file.
    - REDDIT_CLIENT_ID
    - REDDIT_CLIENT_SECRET
    - REDDIT_USER_AGENT
    """
    load_dotenv()
    
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT")

    if not all([client_id, client_secret, user_agent]):
        raise ValueError("Reddit API credentials not found in .env file.")

    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            # Optional: Add username/password for higher rate limits
            # username=os.getenv('REDDIT_USERNAME'),
            # password=os.getenv('REDDIT_PASSWORD')
        )
        # Test the connection by trying to fetch the authenticated user's info
        reddit.user.me()
        print("Successfully connected to Reddit API as authenticated user.")
    except Exception as e:
        print(f"Authenticated connection failed: {e}. Falling back to read-only mode.")
        # Fallback to read-only mode if authentication fails
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        print("Connected to Reddit API in read-only mode.")
        
    return reddit

# --- Data Fetching Functions ---

def get_crypto_posts(reddit: praw.Reddit, subreddit_name: str, limit: int = 100):
    """
    Gets recent posts from a subreddit using PRAW.
    
    Args:
        reddit: An authenticated PRAW Reddit instance.
        subreddit_name: The name of the subreddit to scrape.
        limit: The maximum number of posts to fetch.
    
    Returns:
        A list of PRAW Submission (post) objects.
    """
    try:
        subreddit = reddit.subreddit(subreddit_name)
        posts = list(subreddit.new(limit=limit))
        return posts
    except Exception as e:
        print(f"Error fetching posts from r/{subreddit_name}: {e}")
        return []

def get_post_comments(post: praw.models.Submission):
    """
    Fetches all comments from a PRAW post object, including nested replies.
    
    Args:
        post: A PRAW Submission (post) object.
    
    Returns:
        A list of dictionaries, where each dictionary represents a flattened comment.
    """
    all_comments_data = []
    try:
        # This is the key PRAW command to fetch all comments, including "load more" links
        post.comments.replace_more(limit=None)
        
        # post.comments.list() provides a flattened list of all comment objects
        for comment in post.comments.list():
            all_comments_data.append({
                'id': comment.id,
                'parent_id': comment.parent_id,
                'author': str(comment.author) if comment.author else '[deleted]',
                'body': comment.body,
                'score': comment.score,
                'created': comment.created_utc,
                'depth': comment.depth,
            })
        return all_comments_data
    except Exception as e:
        print(f"Error fetching comments for post {post.id}: {e}")
        return [] 