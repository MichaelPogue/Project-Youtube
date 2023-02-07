'''
Code created by: Michael Pogue
Resources used: Google's Youtube API Tutorials

Note:
As the following code is used as a current business model, I've omitted the 
'''

import os, csv, schedule, datetime, time
import pandas as pd
from schedule import every, repeat
from os import path
from csv import writer
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Set API parameters. 
load_dotenv()
API_KEY = os.getenv('API_KEY')
CHANNEL_ID = os.getenv('CHANNEL_ID')
youtube = build('youtube', 'v3', developerKey = API_KEY)

# Obtain primary data.
class ytStatsBase:
    def __init__(self, API_KEY, CHANNEL_ID, youtube):
        self.API_KEY = API_KEY
        self.CHANNEL_ID = CHANNEL_ID
        self.youtube = youtube

    # Get basic channel statistics.
    def get_channel_stats(self):
        all_data = []

        request = youtube.channels().list(
            part = "snippet,contentDetails,statistics",
            id = CHANNEL_ID
            )
        response = request.execute()

        for i in range (len(response['items'])):
            data = dict(
                result_channel_name = response['items'][i]['snippet']['title'], 
                result_playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                result_channel_subscribers = response['items'][i]['statistics']['subscriberCount'], 
                result_channel_views = response['items'][i]['statistics']['viewCount'], 
                result_channel_total_videos = response['items'][i]['statistics']['videoCount'] 
                )
            all_data.append(data)    

        dfcstats = pd.DataFrame(all_data, columns = [
            'result_playlist_id',
            'result_channel_name', 
            'result_channel_views',
            'result_channel_subscribers',
            'result_channel_total_videos' 
            ]).sort_index(ascending=False)

        return all_data#, channel_name

    # Get indidividual video statistics.
    def get_video_stats(self):
        playlist_id = ytStatsBase.get_channel_stats(youtube)[0]['result_playlist_id']

        request = youtube.playlistItems().list(
            part = 'contentDetails', 
            playlistId = playlist_id, 
            maxResults = 50
        )
        response = request.execute()

        video_ids = []
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
        more_pages = True

        while more_pages:
            if next_page_token is None:
                more_pages = False
            else:
                request = youtube.playlistItems().list(
                    part = 'contentDetails', 
                    playlistId = playlist_id, 
                    maxResults = 50, 
                    pageToken = next_page_token
                )
                response = request.execute()                
                for i in range(len(response['items'])):
                    video_ids.append(response['items'][i]['contentDetails']['videoId'])                
                next_page_token = response.get('nextPageToken')

        all_data = []
        for i in range(0, len(video_ids), 50):
            request = youtube.videos().list(
                part = 'snippet, statistics', 
                id = ','.join(video_ids[i: i + 50]),
                #order = 'date' # Not possible or a feature at this time, 12Dec22.
            )
            response = request.execute()

            for video in response['items']:
                video_stats = dict(
                    result_video_name = video['snippet']['title'],
                    result_video_id = video['id'],                
                    result_video_description = video['snippet']['description'],
                    result_video_upload_time = video['snippet']['publishedAt'],
                    result_video_views = video['statistics']['viewCount'],  
                    result_video_likes = video['statistics']['likeCount'],  
                    result_video_favorites = video['statistics']['favoriteCount'],  
                    result_video_comments = video['statistics']['commentCount']  
                    )
                all_data.append(video_stats)
        return all_data

    # Place channel and video stats into dataframes.
    def create_data_frame(self):
        channel_stats = ytStatsBase.get_channel_stats(youtube)
        video_details = ytStatsBase.get_video_stats(youtube)

        channel_data = pd.DataFrame(channel_stats, columns = [
            'result_playlist_id',
            'result_channel_name', 
            'result_channel_views',
            'result_channel_subscribers',
            'result_channel_total_videos' 
            ]).sort_index(ascending=False).reset_index()
        video_data = pd.DataFrame(video_details, columns = [
            'result_video_id',
            'result_video_upload_time', 
            'result_video_name', 
            'result_video_description', 
            'result_video_views', 
            'result_video_likes', 
            'result_video_comments' 
            ]).sort_index(ascending=False).reset_index()
        return channel_data.drop(['index'], axis=1), video_data.drop(['index'], axis=1)

# Process data into managable CSV data files.
class ytStatsProcess(ytStatsBase):
    def __init__(self, youtube):
        super().__init__(id, youtube)

    # Create base files to scrape all data from channel and videos.
    def create_base_files(self):
        dfcstats, dfvstats = ytStatsBase.create_data_frame(self.youtube)
        channel_name = ytStatsBase.get_channel_stats(self.youtube)[0]['result_channel_name']

        """  Channel Data
        ------------------------------------------------------------------------------------------ """
        channel_data = pd.DataFrame(dfcstats, columns = [
            pd.to_datetime('today').strftime("%y/%m/%d"),
            'result_channel_name', 
            'result_channel_views',
            'result_channel_subscribers',
            'result_channel_total_videos' 
            ]).sort_index(ascending=False).reset_index().drop(['index'], axis=1)
        channel_data
        channel_name = channel_data['result_channel_name'].values[0] 

        channel_data.to_csv(f'{channel_name}(1_channel_data).csv')

        """ Individual Video Data
        ------------------------------------------------------------------------------------------ """
        video_data = pd.DataFrame(dfvstats, columns = [
            'result_video_id',
            'result_video_upload_time', 
            'result_video_name', 
            'result_video_description', 
            'result_video_views', 
            'result_video_likes', 
            'result_video_comments' 
            ]).sort_index(ascending=True).reset_index().drop(['index'], axis=1)

        video_data.to_csv(f'{channel_name}(2_video_data).csv')

ytStatsProcess.create_base_files(youtube)