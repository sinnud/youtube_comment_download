#!/usr/bin/env python3

import csv
import sys
import argparse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def get_video_comments(api_key, video_id, max_results=100):
    """
    Extracts comments from a YouTube video using the YouTube Data API.
    Includes both top-level comments and their replies.
    
    Args:
        api_key (str): YouTube Data API key
        video_id (str): YouTube video ID
        max_results (int): Maximum number of comment threads to retrieve
    
    Returns:
        list: List of comment dictionaries with level information
    """
    youtube = build('youtube', 'v3', developerKey=api_key)
    comments = []
    
    try:
        # Get video comments with replies
        request = youtube.commentThreads().list(
            part='snippet,replies',
            videoId=video_id,
            maxResults=max_results,
            order='relevance'
        )
        
        response = request.execute()
        
        for item in response['items']:
            # Add top-level comment (level 0)
            top_comment = item['snippet']['topLevelComment']['snippet']
            comments.append({
                'level': 0,
                'author': top_comment['authorDisplayName'],
                'comment': top_comment['textDisplay'],
                'like_count': top_comment['likeCount'],
                'published_at': top_comment['publishedAt'],
                'updated_at': top_comment['updatedAt'],
                'parent_id': '',
                'comment_id': item['snippet']['topLevelComment']['id']
            })
            
            # Add replies (level 1) if they exist
            if 'replies' in item:
                total_reply_count = item['snippet']['totalReplyCount']
                
                # Add the replies that come with the commentThread
                for reply in item['replies']['comments']:
                    reply_snippet = reply['snippet']
                    comments.append({
                        'level': 1,
                        'author': reply_snippet['authorDisplayName'],
                        'comment': reply_snippet['textDisplay'],
                        'like_count': reply_snippet['likeCount'],
                        'published_at': reply_snippet['publishedAt'],
                        'updated_at': reply_snippet['updatedAt'],
                        'parent_id': item['snippet']['topLevelComment']['id'],
                        'comment_id': reply['id']
                    })
                
                # If there are more replies than what came with the thread, fetch them
                if total_reply_count > len(item['replies']['comments']):
                    try:
                        replies_request = youtube.comments().list(
                            part='snippet',
                            parentId=item['snippet']['topLevelComment']['id'],
                            maxResults=100
                        )
                        replies_response = replies_request.execute()
                        
                        # Skip the replies we already have
                        existing_reply_ids = {r['id'] for r in item['replies']['comments']}
                        
                        for reply in replies_response['items']:
                            if reply['id'] not in existing_reply_ids:
                                reply_snippet = reply['snippet']
                                comments.append({
                                    'level': 1,
                                    'author': reply_snippet['authorDisplayName'],
                                    'comment': reply_snippet['textDisplay'],
                                    'like_count': reply_snippet['likeCount'],
                                    'published_at': reply_snippet['publishedAt'],
                                    'updated_at': reply_snippet['updatedAt'],
                                    'parent_id': item['snippet']['topLevelComment']['id'],
                                    'comment_id': reply['id']
                                })
                        
                        # Handle pagination for replies if needed
                        while 'nextPageToken' in replies_response:
                            replies_request = youtube.comments().list(
                                part='snippet',
                                parentId=item['snippet']['topLevelComment']['id'],
                                maxResults=100,
                                pageToken=replies_response['nextPageToken']
                            )
                            replies_response = replies_request.execute()
                            
                            for reply in replies_response['items']:
                                reply_snippet = reply['snippet']
                                comments.append({
                                    'level': 1,
                                    'author': reply_snippet['authorDisplayName'],
                                    'comment': reply_snippet['textDisplay'],
                                    'like_count': reply_snippet['likeCount'],
                                    'published_at': reply_snippet['publishedAt'],
                                    'updated_at': reply_snippet['updatedAt'],
                                    'parent_id': item['snippet']['topLevelComment']['id'],
                                    'comment_id': reply['id']
                                })
                    except HttpError as reply_error:
                        print(f"Warning: Could not fetch all replies for comment {item['snippet']['topLevelComment']['id']}: {reply_error}")
                        continue
            
        # Handle pagination if there are more comments
        while 'nextPageToken' in response and len([c for c in comments if c['level'] == 0]) < max_results:
            request = youtube.commentThreads().list(
                part='snippet,replies',
                videoId=video_id,
                maxResults=min(max_results - len([c for c in comments if c['level'] == 0]), 100),
                order='relevance',
                pageToken=response['nextPageToken']
            )
            response = request.execute()
            
            for item in response['items']:
                # Add top-level comment (level 0)
                top_comment = item['snippet']['topLevelComment']['snippet']
                comments.append({
                    'level': 0,
                    'author': top_comment['authorDisplayName'],
                    'comment': top_comment['textDisplay'],
                    'like_count': top_comment['likeCount'],
                    'published_at': top_comment['publishedAt'],
                    'updated_at': top_comment['updatedAt'],
                    'parent_id': '',
                    'comment_id': item['snippet']['topLevelComment']['id']
                })
                
                # Add replies (level 1) if they exist
                if 'replies' in item:
                    total_reply_count = item['snippet']['totalReplyCount']
                    
                    # Add the replies that come with the commentThread
                    for reply in item['replies']['comments']:
                        reply_snippet = reply['snippet']
                        comments.append({
                            'level': 1,
                            'author': reply_snippet['authorDisplayName'],
                            'comment': reply_snippet['textDisplay'],
                            'like_count': reply_snippet['likeCount'],
                            'published_at': reply_snippet['publishedAt'],
                            'updated_at': reply_snippet['updatedAt'],
                            'parent_id': item['snippet']['topLevelComment']['id'],
                            'comment_id': reply['id']
                        })
                    
                    # If there are more replies than what came with the thread, fetch them
                    if total_reply_count > len(item['replies']['comments']):
                        try:
                            replies_request = youtube.comments().list(
                                part='snippet',
                                parentId=item['snippet']['topLevelComment']['id'],
                                maxResults=100
                            )
                            replies_response = replies_request.execute()
                            
                            # Skip the replies we already have
                            existing_reply_ids = {r['id'] for r in item['replies']['comments']}
                            
                            for reply in replies_response['items']:
                                if reply['id'] not in existing_reply_ids:
                                    reply_snippet = reply['snippet']
                                    comments.append({
                                        'level': 1,
                                        'author': reply_snippet['authorDisplayName'],
                                        'comment': reply_snippet['textDisplay'],
                                        'like_count': reply_snippet['likeCount'],
                                        'published_at': reply_snippet['publishedAt'],
                                        'updated_at': reply_snippet['updatedAt'],
                                        'parent_id': item['snippet']['topLevelComment']['id'],
                                        'comment_id': reply['id']
                                    })
                            
                            # Handle pagination for replies if needed
                            while 'nextPageToken' in replies_response:
                                replies_request = youtube.comments().list(
                                    part='snippet',
                                    parentId=item['snippet']['topLevelComment']['id'],
                                    maxResults=100,
                                    pageToken=replies_response['nextPageToken']
                                )
                                replies_response = replies_request.execute()
                                
                                for reply in replies_response['items']:
                                    reply_snippet = reply['snippet']
                                    comments.append({
                                        'level': 1,
                                        'author': reply_snippet['authorDisplayName'],
                                        'comment': reply_snippet['textDisplay'],
                                        'like_count': reply_snippet['likeCount'],
                                        'published_at': reply_snippet['publishedAt'],
                                        'updated_at': reply_snippet['updatedAt'],
                                        'parent_id': item['snippet']['topLevelComment']['id'],
                                        'comment_id': reply['id']
                                    })
                        except HttpError as reply_error:
                            print(f"Warning: Could not fetch all replies for comment {item['snippet']['topLevelComment']['id']}: {reply_error}")
                            continue
                
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
            fieldnames = ['level', 'author', 'comment', 'like_count', 'published_at', 'updated_at', 'parent_id', 'comment_id']
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