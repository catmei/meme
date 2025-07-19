import threading
import os
import praw
import json
from dotenv import load_dotenv
from kafka import KafkaProducer

# --- Initialization ---

def initialize_praw():
    """Initializes and returns an authenticated PRAW Reddit instance from .env credentials."""
    load_dotenv()
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT")
    if not all([client_id, client_secret, user_agent]):
        raise ValueError("Reddit API credentials not found in .env file.")
    print("Connecting to Reddit...")
    reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
    print("Successfully connected to Reddit API.")
    return reddit

def initialize_kafka_producer():
    """Initializes and returns a KafkaProducer instance."""
    print("Connecting to Kafka broker...")
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Successfully connected to Kafka.")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        raise

# --- Data Serialization ---

def serialize_post(post):
    """Serializes a PRAW Submission object into a dictionary for Kafka."""
    return {
        'id': post.id,
        'title': post.title,
        'selftext': post.selftext,
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

def serialize_comment(comment):
    """Serializes a PRAW Comment object into a dictionary for Kafka."""
    return {
        'id': comment.id,
        'post_id': comment.submission.id,
        'parent_id': comment.parent_id,
        'author': str(comment.author) if comment.author else '[deleted]',
        'body': comment.body,
        'score': comment.score,
        'created_utc': comment.created_utc,
        'is_submitter': comment.is_submitter
    }

# --- Streaming and Publishing Logic ---

def watch_new_posts(reddit, producer, topic):
    """Streams new posts and publishes them to a Kafka topic."""
    print(f"--- Starting Post Stream -> Kafka Topic: {topic} ---")
    for post in reddit.subreddit("ethtrader").stream.submissions(skip_existing=True):
        post_data = serialize_post(post)
        producer.send(topic, value=post_data)
        print("\n[POST PUBLISHED]:")
        print(json.dumps(post_data, indent=2))
        print("-" * 40)

def watch_comments(reddit, producer, topic):
    """Streams new comments and publishes them to a Kafka topic."""
    print(f"--- Starting Comment Stream -> Kafka Topic: {topic} ---")
    for comment in reddit.subreddit("ethtrader").stream.comments(skip_existing=True):
        comment_data = serialize_comment(comment)
        producer.send(topic, value=comment_data)
        print("\n[COMMENT PUBLISHED]:")
        print(json.dumps(comment_data, indent=2))
        print("-" * 40)

# --- Main Execution ---

if __name__ == "__main__":
    try:
        reddit = initialize_praw()
        producer = initialize_kafka_producer()
        
        POST_TOPIC = 'reddit_posts'
        COMMENT_TOPIC = 'reddit_comments'

        # Run comment stream in a background thread
        comment_thread = threading.Thread(
            target=watch_comments, 
            args=(reddit, producer, COMMENT_TOPIC), 
            daemon=True
        )
        comment_thread.start()

        # Run post stream in the main thread (blocks forever)
        watch_new_posts(reddit, producer, POST_TOPIC)
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'producer' in locals() and producer:
            print("Closing Kafka producer.")
            producer.close() 