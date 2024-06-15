import googleapiclient.discovery
import streamlit as st
import mysql.connector 
import pandas as pd
from isodate import parse_duration

mydb = mysql.connector.connect(
 host="localhost",
 user="root",
 password="NISHa1413@@@",
 database ="youtube"
 )
mycursor = mydb.cursor()
api_service_name = "youtube"
api_version = "v3"
api_key='AIzaSyBxB-v0x193Teg0J8SQD58lphg_wJdLPMk'
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

def get_channel_info(channel_id):
        response=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id).execute()
        for i in response['items']:
            data = {
            'Channel_name':i['snippet']['title'],
            'Channel_Id':channel_id,
            'Subscribers_count':i['statistics'].get('subscribersCount'),
            'Channel_views':i['statistics'].get('viewCount'),
            'Channel_des':i['snippet'].get('description',''),
        }
        query='''INSERT INTO channels (Channel_name, Channel_Id, Subscribers_count, Channel_views, Channel_des) VALUES (%s, %s, %s, %s, %s)'''
        mycursor.execute(query,(data['Channel_name'],
                        data['Channel_Id'], data['Subscribers_count'], data['Channel_views'], data['Channel_des']))
mydb.commit()

    


def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(part="contentDetails",
                                     id=channel_id).execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1 = youtube.playlistItems().list(
            part="snippet",
            playlistId= Playlist_Id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids
#video_Ids=get_videos_ids(channel_id)
#video_Ids

def get_video_info(video_ids):
    video_data=[]
    def dur(duration_str):
        duration_seconds=parse_duration(duration_str).total_seconds()
        return duration_seconds
    for video_id in video_ids:
        request=youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=video_id

        )
        response= request.execute()
        date=list(response['items'][0]['snippet']['publishedAt'].split('T'))
        for item in response['items']:
            data ={
                'Channels_name':item['snippet']['channelTitle'],
                'Channel_Id':item['snippet']['channelId'],
                'Video_Id' :video_id,
                'Title':item['snippet']['title'],
                'Description':item['snippet'].get('description'),
                'Published_at':date[0],
                'Duration':str(dur(item['contentDetails']['duration'])),
                'View_count':item['statistics'].get('viewCount'),
                'Like_count':item['statistics'].get('likeCount',0),
                'Dislike_count':item['statistics'].get('dislikeCount',0),
                'Comment_count':item['statistics'].get('commentCount',0),
                'Favorite_count':item['statistics']['favoriteCount'],
                'Caption_status':item['contentDetails']['caption']
            }
        video_data.append(data)
    video=pd.DataFrame(video_data)
    for _, row in video.iterrows():
        query="INSERT INTO videos (Channels_name, Channel_Id, Video_Id, Title, Description, Published_at, Duration, View_count, Like_count,Dislike_count,Comment_count, Favorite_count, Caption_Status) VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        mycursor.execute(query,(row['Channels_name'],row['Channel_Id'],row['Video_Id'], row['Title'],
                            row['Description'],row['Published_at'],row['Duration'],
                            row['View_count'],row['Like_count'],row['Dislike_count'],row['Comment_count'], row['Favorite_count'], 
                            row['Caption_status']))
                
        mydb.commit()
    
def get_comment_info(video_ids):
    Comment_data=[]
    for video_id in video_ids:
        try:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50,
            )
            response=request.execute()

            for item in response['items']:
                data={
                    'Comment_Id':item['snippet']['topLevelComment']['id'],
                    'Video_Id':video_id,
                    'Comment_Text':item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_Published':item['snippet']['topLevelComment']['snippet']['publishedAt'].split('T')[0]
                }
                Comment_data.append(data)
        except:
            pass
    
    comment=pd.DataFrame(Comment_data)
    for _, row1 in comment.iterrows():
        query="INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published) VALUES (%s, %s, %s, %s, %s)"
        mycursor.execute(query,(row1['Comment_Id'], 
                                row1['Video_Id'], row1['Comment_Text'],
                                row1['Comment_Author'], row1['Comment_Published']))
    mydb.commit()
    
    
st.title("YouTube Data Harvesting and Warsheing")
channel_id = st.text_input("Enter YouTube Channel ID:")


if st.button("Data Extraction and stored"):
    mycursor.execute('select Channel_id from channels')
    out =  [i[0]for i in mycursor.fetchall()]
    if channel_id in out:
        st.success("channel id already exists")
    else:
        get_channel_info(channel_id)
        video_Ids=get_videos_ids(channel_id)
        get_video_info(video_Ids)
        get_comment_info(video_Ids)
        st.success("success")

query_options = [
    "What are the names of all the videos and their corresponding channels?",
    "Which channels have the most number of videos, and how many videos do they have?",
    "What are the top 10 most viewed videos and their respective channels?",
    "How many comments were made on each video, and what are their corresponding video names?",
    "Which videos have the highest number of likes, and what are their corresponding channel names?",
    "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "What is the total number of views for each channel, and what are their corresponding channel names?",
    "What are the names of all the channels that have published videos in the year 2022?",
    "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "Which videos have the highest number of comments, and what are their corresponding channel names?"
    ]
selected_query = st.selectbox("Select Question:", query_options)

if st.button("Execute"):
    
    if selected_query == query_options[0]:
        query_result = pd.read_sql_query("SELECT Title,Channels_name FROM videos",mydb)
    elif selected_query == query_options[1]:
        query_result = pd.read_sql_query("SELECT Channel_name, Channel_views FROM channels ORDER BY Channel_views DESC", mydb)
    elif selected_query == query_options[2]:
        query_result = pd.read_sql_query("SELECT Channels_name, Title, View_count FROM videos ORDER BY View_count DESC LIMIT 10;", mydb)
    elif selected_query == query_options[3]:
        query_result = pd.read_sql_query("SELECT Title, Comment_count,Channels_name FROM videos", mydb)
    elif selected_query == query_options[4]:
        query_result = pd.read_sql_query("SELECT Channels_name, Title,Like_count FROM videos ORDER BY Like_count DESC LIMIT 1", mydb)
    elif selected_query == query_options[5]:
        query_result = pd.read_sql_query("SELECT Title, Like_count,Dislike_count FROM videos", mydb)
    elif selected_query == query_options[6]:
        query_result = pd.read_sql_query("SELECT Channel_name,Channel_views FROM channels", mydb)
        
    elif selected_query == query_options[7]:
        query_result = pd.read_sql_query("SELECT Channels_name,Title,Published_at FROM videos WHERE extract(year from Published_at)=2022", mydb)
    elif selected_query == query_options[8]:
        query_result = pd.read_sql_query("SELECT Channels_name, AVG(Duration)FROM videos GROUP BY Channels_name", mydb)
    elif selected_query == query_options[9]:
        query_result = pd.read_sql_query("SELECT Channels_name, Title,Comment_count FROM videos  ORDER BY Comment_count DESC", mydb)
    mydb.close()

    st.dataframe(query_result)

    


     
    
      
