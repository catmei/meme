import time
import csv
import os
from scraper_utils import initialize_praw, get_crypto_posts, get_post_comments

def main():
    """
    Simple main function to demo the PRAW scraper and save to relational CSV files.
    This is a one-time script for testing purposes.
    """
    print("=== Simple Reddit Crypto Scraper to CSV (PRAW Version) ===")

    try:
        reddit = initialize_praw()
    except ValueError as e:
        print(f"Stopping script: {e}")
        print("Please ensure REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, and REDDIT_USER_AGENT are set in a .env file.")
        return

    POSTS_FILE = 'posts.csv'
    COMMENTS_FILE = 'comments.csv'

    POST_HEADERS = ['id', 'title', 'score', 'author', 'subreddit', 'url', 'num_comments', 'created', 'permalink']
    COMMENT_HEADERS = ['id', 'post_id', 'parent_id', 'author', 'body', 'score', 'created', 'depth']

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

        print("Getting 2 posts from r/ethtrader using PRAW...")
        ethtrader_posts = get_crypto_posts(reddit, 'ethtrader', limit=2)

        if not ethtrader_posts:
            print("No posts found!")
            return

        total_posts_saved = 0
        total_comments_saved = 0

        for i, post in enumerate(ethtrader_posts, 1):
            print(f"\n--- Processing Post {i}: {post.title} ---")

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

            print("Getting comments with PRAW...")
            comments = get_post_comments(post)
            
            print(f"Found {len(comments)} comments/replies. Saving to {COMMENTS_FILE}...")
            for comment in comments:
                comment['post_id'] = post.id
                comments_writer.writerow(comment)
                total_comments_saved += 1
            
            time.sleep(1)

    print(f"\n✅ Done. Saved {total_posts_saved} posts to {POSTS_FILE} and {total_comments_saved} comments/replies to {COMMENTS_FILE}.")


if __name__ == "__main__":
    main() 