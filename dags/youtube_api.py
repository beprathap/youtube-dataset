import json
import boto3
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

class YouTubeAPI:

    def __init__(self, api_key):
        # Builds a new instance of YouTube.
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.s3_client = boto3.client('s3')
        self.s3_bucket_name = 'airflow-practice-bucket'

    def get_channel_stats(self, channel_ids):
        try:
            request = self.youtube.channels().list(
                part='snippet, contentDetails, statistics',
                id=','.join(channel_ids)
            )
            response = request.execute()
            return response['items']
        except HttpError as e:
            print(f'An error occurred: {e}')
            return None

    def get_latest_videos(self, channel_id, max_results=10):
        try:
            request = self.youtube.search().list(
                part='id, snippet',
                channelId=channel_id,
                type='video',
                order='date',
                maxResults=max_results
            )
            response = request.execute()
            video_ids = [item['id']['videoId'] for item in response['items']]
            return self.get_video_details(video_ids)
        except HttpError as e:
            print(f'An error occurred: {e}')
            return None

    def get_video_details(self, video_ids):
        try:
            request = self.youtube.videos().list(
                part='snippet, contentDetails, statistics',
                id=','.join(video_ids)
            )
            response = request.execute()
            return response['items']
        except HttpError as e:
            print(f'An error occurred: {e}')
            return None


    def get_video_comments(self, video_id, max_results=50):
        try:
            request = self.youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=max_results
            )
            response = request.execute()
            return response['items']
        except HttpError as e:
            print(f'An error occurred: {e}')
            return None

    def get_video_categories(self, video_ids):
        try:
            categories = {}
            for video_id in video_ids:
                request = self.youtube.videos().list(
                    part='snippet',
                    id=video_id
                )
                response = request.execute()
                if 'items' in response and len(response['items']) > 0:
                    category_id = response['items'][0]['snippet']['categoryId']
                    category_request = self.youtube.videoCategories().list(
                        part='snippet',
                        id=category_id
                    )
                    category_response = category_request.execute()
                    if 'items' in category_response and len(category_response['items']) > 0:
                        category_title = category_response['items'][0]['snippet']['title']
                        categories[video_id] = {
                            'category_id': int(category_id),
                            'title': category_title
                        }
            return categories
        except HttpError as e:
            print(f'An error occurred: {e}')
            return None

    # Add more methods for other API calls (e.g., get_video_stats, get_comments)

    def save_raw_json_to_s3(self, data, s3_key):
        try:
            # Convert data to JSON string
            json_data = json.dumps(data, indent=4)

            # Upload JSON to S3 bucket
            self.s3_client.put_object(
                Bucket = self.s3_bucket_name,
                Key = s3_key,
                Body = json_data,
                ContentType = 'application/json'
            )
            print(f"Data successfully uploaded to S3: {s3_key}")
        except Exception as e:
            print(f"Failed to upload data to S3: {e}")