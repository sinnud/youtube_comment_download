#!/usr/bin/env python3

import csv
import sys
import argparse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def get_video_comments(api_key, video_id, max_results=100):
    """
    Extracts comments from a YouTube video using the YouTube Data API.
    
    Args:
        api_key (str): YouTube Data API key
        video_id (str): YouTube video ID
        max_results (int): Maximum number of comments to retrieve
    
    Returns:
        list: List of comment dictionaries
    """
    youtube = build('youtube', 'v3', developerKey=api_key)
    comments = []
    
    try:
        # Get video comments
        request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=max_results,
            order='relevance'
        )
        
        response = request.execute()
        
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comments.append({
                'author': comment['authorDisplayName'],
                'comment': comment['textDisplay'],
                'like_count': comment['likeCount'],
                'published_at': comment['publishedAt'],
                'updated_at': comment['updatedAt']
            })
            
        # Handle pagination if there are more comments
        while 'nextPageToken' in response and len(comments) < max_results:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=min(max_results - len(comments), 100),
                order='relevance',
                pageToken=response['nextPageToken']
            )
            response = request.execute()
            
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                comments.append({
                    'author': comment['authorDisplayName'],
                    'comment': comment['textDisplay'],
                    'like_count': comment['likeCount'],
                    'published_at': comment['publishedAt'],
                    'updated_at': comment['updatedAt']
                })
                
    except HttpError as e:
        print(f"An HTTP error occurred: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
        
    return comments


def save_comments_to_csv(comments, video_id, filename=None):
    """
    Saves comments to a CSV file.
    
    Args:
        comments (list): List of comment dictionaries
        video_id (str): YouTube video ID (used for default filename)
        filename (str): Optional custom filename
    """
    if not filename:
        filename = f"youtube_comments_{video_id}.csv"
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['author', 'comment', 'like_count', 'published_at', 'updated_at']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for comment in comments:
                writer.writerow(comment)
                
        print(f"Successfully saved {len(comments)} comments to {filename}")
        
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Extract YouTube comments and save to CSV')
    parser.add_argument('api_key', help='YouTube Data API key')
    parser.add_argument('video_id', help='YouTube video ID')
    parser.add_argument('--max-comments', type=int, default=100, 
                       help='Maximum number of comments to extract (default: 100)')
    parser.add_argument('--output', '-o', help='Output CSV filename')
    
    args = parser.parse_args()
    
    print(f"Extracting comments from video: {args.video_id}")
    print(f"Maximum comments: {args.max_comments}")
    
    comments = get_video_comments(args.api_key, args.video_id, args.max_comments)
    
    if not comments:
        print("No comments found or unable to retrieve comments.")
        sys.exit(1)
    
    save_comments_to_csv(comments, args.video_id, args.output)


if __name__ == "__main__":
    main()