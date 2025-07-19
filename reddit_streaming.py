import threading
import os
import praw
from dotenv import load_dotenv

def initialize_praw():
    """Initializes and returns an authenticated PRAW Reddit instance from .env credentials."""
    load_dotenv()
    
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT")

    if not all([client_id, client_secret, user_agent]):
        raise ValueError("Reddit API credentials not found in .env file.")

    print("Connecting to Reddit...")
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )
    print("Successfully connected to Reddit API.")
    return reddit

# --- Helper Functions for Printing ---

def print_post_details(post):
    """Prints a structured view of the recommended post attributes."""
    print("\n" + "="*40)
    print(f"-> NEW POST | Title: {post.title}")
    print("="*40)
    details = {
        'id': post.id,
        'title': post.title,
        'selftext': f"'{post.selftext[:100].replace('\n', ' ').replace('\r', ' ')}...'" if post.selftext else "''",
        'score': post.score,
        'upvote_ratio': post.upvote_ratio,
        'num_comments': post.num_comments,
        'author': str(post.author) if post.author else '[deleted]',
        'created_utc': post.created_utc,
        'url': post.url,
        'permalink': post.permalink,
        'link_flair_text': post.link_flair_text,
        'stickied': post.stickied
    }
    for key, value in details.items():
        print(f"  - {key:<16}: {value}")
    print("="*40)

def print_comment_details(comment):
    """Prints a structured view of the recommended comment attributes."""
    details = {
        'id': comment.id,
        'post_id': comment.submission.id,
        'parent_id': comment.parent_id,
        'author': str(comment.author) if comment.author else '[deleted]',
        'body': f"'{comment.body[:80].replace('\n', ' ').replace('\r', ' ')}...'",
        'score': comment.score,
        'created_utc': comment.created_utc,
        'is_submitter': comment.is_submitter
    }
    print("\n" + "-"*40)
    print(f"-> New Comment by {details['author']} on post {details['post_id']}")
    print("-"*40)
    for key, value in details.items():
        print(f"  - {key:<12}: {value}")
    print("-"*40)

# --- Main Script ---

reddit = initialize_praw()

def watch_new_posts():
    """Stream that watches for new posts and prints their details."""
    print("--- Starting Post Stream ---")
    for post in reddit.subreddit("ethtrader").stream.submissions(skip_existing=True):
        print_post_details(post)

def watch_comments():
    """Stream that watches for new comments and prints their details."""
    print("--- Starting Comment Stream ---")
    for comment in reddit.subreddit("ethtrader").stream.comments(skip_existing=True):
        print_comment_details(comment)

# Comments in background, Posts in main
comment_thread = threading.Thread(target=watch_comments, daemon=True)
comment_thread.start()
watch_new_posts()