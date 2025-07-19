import time
import csv
import os
from scraper_utils import initialize_praw, get_crypto_posts, get_post_comments

# --- Configuration ---
SUBREDDIT = 'ethtrader'
POSTS_FILE = 'data/posts.csv'
COMMENTS_FILE = 'data/comments.csv'
POST_HEADERS = ['id', 'title', 'score', 'author', 'subreddit', 'url', 'num_comments', 'created', 'permalink']
COMMENT_HEADERS = ['id', 'post_id', 'parent_id', 'author', 'body', 'score', 'created', 'depth']

def scrape_recent_posts(timespan_seconds=70):
    """
    Scrapes for new posts using PRAW, filters by time, and saves them.
    This function is designed to be called by a scheduler like Airflow.
    
    Args:
        timespan_seconds (int): The lookback period in seconds to find new posts. Defaults to 60.
    """
    print(f"--- Starting PRAW scraping cycle (timespan: {timespan_seconds}s) ---")

    try:
        reddit = initialize_praw()
    except ValueError as e:
        print(f"Stopping cycle: {e}")
        print("Please ensure REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, and REDDIT_USER_AGENT are set in a .env file.")
        return

    # 1. Get the most recent posts from Reddit
    print(f"Fetching recent posts from r/{SUBREDDIT} with PRAW...")
    recent_posts = get_crypto_posts(reddit, SUBREDDIT, limit=100)
    
    if not recent_posts:
        print("No recent posts found from API.")
        return

    # 2. Filter for posts that are truly new
    start_time = time.time() - timespan_seconds
    new_posts = [
        post for post in recent_posts 
        if post.created_utc >= start_time
    ]

    if not new_posts:
        print(f"No new posts within the last {timespan_seconds} seconds to process.")
        print("--- Scraping cycle complete ---")
        return

    print(f"Found {len(new_posts)} new posts to process.")

    # 3. Append new data to CSV files
    posts_file_exists = os.path.exists(POSTS_FILE)
    comments_file_exists = os.path.exists(COMMENTS_FILE)

    with open(POSTS_FILE, 'a', newline='', encoding='utf-8') as posts_file, \
         open(COMMENTS_FILE, 'a', newline='', encoding='utf-8') as comments_file:

        posts_writer = csv.DictWriter(posts_file, fieldnames=POST_HEADERS)
        comments_writer = csv.DictWriter(comments_file, fieldnames=COMMENT_HEADERS)

        if not posts_file_exists:
            posts_writer.writeheader()
        
        if not comments_file_exists:
            comments_writer.writeheader()
        
        total_posts_saved = 0
        total_comments_saved = 0

        for post in new_posts:
            print(f"  -> Processing post: {post.title}")
            
            # Save post by accessing PRAW object attributes
            post_to_save = {
                'id': post.id,
                'title': post.title,
                'score': post.score,
                'author': str(post.author) if post.author else '[deleted]',
                'subreddit': str(post.subreddit),
                'url': post.url,
                'num_comments': post.num_comments,
                'created': post.created_utc,
                'permalink': post.permalink
            }
            posts_writer.writerow(post_to_save)
            total_posts_saved += 1

            # Get and save all comments/replies
            comments = get_post_comments(post)
            for comment in comments:
                comment['post_id'] = post.id
                comments_writer.writerow(comment)
                total_comments_saved += 1
            
            time.sleep(1) # Be nice to Reddit's API

    print(f"\n✅ Success! Saved {total_posts_saved} new posts and {total_comments_saved} new comments/replies.")
    print("--- Scraping cycle complete ---")


if __name__ == "__main__":
    # This allows you to run the script directly for testing purposes.
    # In production, Airflow will call the scrape_recent_posts() function.
    scrape_recent_posts(timespan_seconds=3600*4) 