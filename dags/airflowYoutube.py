from datetime import datetime
from config import API_KEY, CHANNEL_IDS
from youtube_api import YouTubeAPI

def youtube_dataset_extractor():
    # Initialize YouTube API
    api = YouTubeAPI(API_KEY)

    # Get current timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Extract and save raw JSON
    channel_stats = api.get_channel_stats(CHANNEL_IDS)

    # Extract and save channel stats
    if channel_stats:
        s3_key = f"data/raw/{timestamp}/channel_stats_{timestamp}.json"
        api.save_raw_json_to_s3(channel_stats, s3_key)
        print(f"Raw data saved to {s3_key}")
    else:
        print("Failed to retrieve channel_stats data")

    # Extract and save video details for each channel
    all_video_details = []
    video_ids = [] # List to store all video IDs
    for channel_id in CHANNEL_IDS:
        latest_videos = api.get_latest_videos(channel_id)
        if latest_videos:
            print(f"Retrieved video details for channel {channel_id}")
            all_video_details.extend(latest_videos) # Add videos to the master list

            # Extract video IDs and add them to the video_ids list
            video_ids.extend([video['id'] for video in latest_videos])

    # Save all video details to a single JSON file
    if all_video_details:
        s3_key = f"data/raw/{timestamp}/video_details_{timestamp}.json"
        api.save_raw_json_to_s3(all_video_details, s3_key)
        print(f"Raw data saved to {s3_key}")
    else:
        print("Failed to retrieve video_details data")

    # Extract and save video comments
    all_comments = []
    for video in all_video_details:
        video_id = video['id']
        comments = api.get_video_comments(video_id)
        if comments:
            print(f"Retrieved comments for video {video_id}")
            for comment in comments:
                comment_data = {
                    'video_id': video_id,
                    'comment_id': comment['id'],
                    'author_name': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'like_count': comment['snippet']['topLevelComment']['snippet']['likeCount'],
                    'published_at': comment['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                all_comments.append(comment_data)
        else:
            print(f"Failed to retrieve comments for video {video_id}")

    # Save all comments to a single JSON file
    if all_comments:
        s3_key = f"data/raw/{timestamp}/video_comments_{timestamp}.json"
        api.save_raw_json_to_s3(all_comments, s3_key)
        print(f"Video comments saved to {s3_key}")

    # Extract and save video/channel stats

    # Extract and save video categories
    video_categories = api.get_video_categories(video_ids)
    if video_categories:
        s3_key = f"data/raw/{timestamp}/video_categories_{timestamp}.json"
        api.save_raw_json_to_s3(video_categories, s3_key)
        print(f"Video categories saved to {s3_key}")
    else:
        print("Failed to retrieve video categories")

if __name__ == "__main__":
    youtube_dataset_extractor()