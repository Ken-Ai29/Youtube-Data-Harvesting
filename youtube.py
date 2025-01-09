from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import isodate
from datetime import timedelta
import streamlit as st

# API key connection
def Api_connect():
    api_ID = 'AIzaSyAyUD9VnxSAs6VDHDr9Q8vBYEQJCU0vApg'
    api_service_name = 'youtube'
    api_version = 'v3'
    youtube = build(api_service_name, api_version, developerKey=api_ID)
    return youtube
youtube = Api_connect()

# get channel information
def get_channel_info(channel_id):
      request = youtube.channels().list(part="snippet,ContentDetails,statistics",id = channel_id)
      response = request.execute()

      for i in response['items']:
            data = dict(Channel_Name = i['snippet']['title'],
                  Channel_ID = i['id'],
                  Channel_Description = i['snippet']['description'],
                  Channel_Creation_Date = i['snippet']['publishedAt'],
                  Channel_Thumbnail = i['snippet']['thumbnails']['default']['url'],
                  Channel_View_Count = i['statistics']['viewCount'],
                  Channel_Subscriber_Count = i['statistics']['subscriberCount'],
                  Channel_Video_Count = i['statistics']['videoCount'],
                  Playlist_ID = i['contentDetails']['relatedPlaylists']['uploads'])
      return data

#get video ID:
def get_video_ids(channel_id):

    video_ids =[]
    response = youtube.channels().list(id = channel_id,
                                        part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        
        response1 = youtube.playlistItems().list(part='contentDetails',playlistId = playlist_id,maxResults =50,pageToken=next_page_token).execute()
        
        for i in response1['items']:
            video_ids.append(i['contentDetails']['videoId'])
        next_page_token = response1.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


# get video information
def get_video_info(video_ids):
        Video_data = []

        for video_id in video_ids:
                request = youtube.videos().list(part="snippet,ContentDetails,statistics",id = video_id)
                response = request.execute()

                for item in response['items']:
                        data = dict(Channel_name = item['snippet']['channelTitle'],
                                Channel_ID = item['snippet']['channelId'],
                                Video_ID = item['id'],
                                Video_Title = item['snippet']['title'],
                                Video_Description = item['snippet'].get('description'),
                                Video_Publish_Date = item['snippet']['publishedAt'],
                                Video_Duration = item['contentDetails']['duration'],
                                Video_tags = item['snippet'].get('tags'),
                                Video_Thumbnail = item['snippet']['thumbnails']['default']['url'],
                                Video_View_Count = item['statistics'].get('viewCount'),
                                Video_Like_Count = item['statistics'].get('likeCount'),
                                Video_favorite_count = item['statistics'].get('favoriteCount'),
                                Video_Comment_Count = item['statistics'].get('commentCount'),
                                Video_definition = item['contentDetails']['definition'],
                                Video_Caption = item['contentDetails']['caption'],)
                        Video_data.append(data)
        return Video_data


#get_comment_info:
from googleapiclient.errors import HttpError  

def get_comment_info(video_ids):
    try:
        comment_data = []
        for video_id in video_ids:
            
            video_request = youtube.videos().list(part="snippet", id=video_id)
            video_response = video_request.execute()
            
            if "items" in video_response and len(video_response["items"]) > 0:
                video_title = video_response["items"][0]["snippet"]["title"]
            else:
                video_title = "Unknown Title"
            
            try:
                comment_request = youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=100)
                comment_response = comment_request.execute()

                for item in comment_response.get("items", []): 
                    snippet = item["snippet"]["topLevelComment"]["snippet"]
                    data = dict(
                        Video_ID=snippet["videoId"],
                        Video_Title=video_title,  # Include the video title
                        comment_id=item["snippet"]["topLevelComment"]["id"],
                        comment_text=snippet["textDisplay"],
                        comment_author=snippet["authorDisplayName"],
                        comment_like_count=snippet["likeCount"],
                        comment_publish_date=snippet["publishedAt"]
                    )
                    comment_data.append(data)
            except HttpError as e:
                if e.resp.status == 403:
                    print(f"Comments are disabled for video ID: {video_id}. Skipping.")
                else:
                    print(f"An unexpected error occurred for video ID {video_id}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return comment_data            


# get_playlist_info:

def get_playlist_info(channel_id):
    next_page_token = None
    playlist_data = []

    while True:
        request = youtube.playlists().list(part = 'snippet,contentDetails',channelId = channel_id ,maxResults = 50,pageToken = next_page_token)
        response = request.execute() 

        for item in response['items']:
            data = dict(Playlist_ID = item['id'],
                        Playlist_Title = item['snippet']['title'],
                        Playlist_Description = item['snippet']['description'],
                        Playlist_Publish_Date = item['snippet']['publishedAt'],
                        Playlist_Channel_ID = item['snippet']['channelId'],
                        Playlist_Channel_Title = item['snippet']['channelTitle'],
                        Playlist_Video_Count = item['contentDetails']['itemCount'])
            playlist_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data        

client = pymongo.MongoClient("mongodb+srv://Kenny_Ai:kenny_Ai@cluster0.mh35r.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client['Youtube']

# Inserting data into the collection
def Channel_full_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    video_ids = get_video_ids(channel_id)
    video_info = get_video_info(video_ids)
    comment_info = get_comment_info(video_ids)
    col1 = db['Channel_Details']
    col1.insert_one({'Channel_Details':ch_details, 'Playlist_Details':pl_details, 'Video_Details':video_info, 'Comment_Details':comment_info})
    return 'upload completed successfully'

# for table creation for Channels in postgresql
def channels_table(channel_name_single):
    mydb = psycopg2.connect(host = "localhost",user= "postgres",password = "root",database = "Youtube_data_harvesting",port = "5432")
    cursor = mydb.cursor()

    
    create_query = '''create table if not exists channels( channel_name varchar(100), channel_id varchar(100) primary key,
                                    channel_description text, channel_creation_date date, channel_thumbnail text, 
                                    channel_view_count bigint, channel_subscriber_count int, channel_video_count int, playlist_id varchar(100));'''
    cursor.execute(create_query)
    mydb.commit()
    

    single_channel_detail = []
    col1 = db['Channel_Details']
    for ch_data in col1.find({'Channel_Details.Channel_Name': channel_name_single},{'_id':0,'Channel_Details':1}):
        single_channel_detail.append(ch_data["Channel_Details"])

    df_single_channel_detail = pd.DataFrame(single_channel_detail)



    for index, row in df_single_channel_detail.iterrows():
        insert_query = '''insert into channels(channel_name,channel_id,channel_description,channel_creation_date,
                                            channel_thumbnail,channel_view_count,channel_subscriber_count,
                                            channel_video_count,playlist_id) 
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
        
        row['Channel_View_Count'] = min(int(row['Channel_View_Count']), 2147483647)
        values = (row['Channel_Name'], row['Channel_ID'], row['Channel_Description'], row['Channel_Creation_Date'],
                row['Channel_Thumbnail'], row['Channel_View_Count'], row['Channel_Subscriber_Count'],
                row['Channel_Video_Count'], row['Playlist_ID'])
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            news = f'The provided channel name {channel_name_single} already exists in the database'
            return news




# Table creation for playlist in postgresql
def playlist_table(channel_name_single):
    mydb = psycopg2.connect(host = "localhost",user= "postgres",password = "root",database = "Youtube_data_harvesting",port = "5432")
    cursor = mydb.cursor()

    create_query = '''create table if not exists playlists(playlist_id varchar(100) primary key,
                                    playlist_title varchar(100), playlist_description text, playlist_publish_date timestamp,
                                    playlist_channel_id varchar(100), playlist_channel_title varchar(100), playlist_video_count int);'''
    cursor.execute(create_query)
    mydb.commit()

    single_playlist_details =[]
    db = client['Youtube']
    col1 = db['Channel_Details']
    for pl_data in col1.find({'Channel_Details.Channel_Name': channel_name_single},{'_id':0,'Playlist_Details':1}):
        single_playlist_details.extend(pl_data["Playlist_Details"])

    df_single_channel_playlist_detail = pd.DataFrame(single_playlist_details)

    for index, row in  df_single_channel_playlist_detail.iterrows():
        insert_query = '''insert into playlists(playlist_id,playlist_title,playlist_description,playlist_publish_date,
                                            playlist_channel_id,playlist_channel_title,playlist_video_count) 
                                            values(%s,%s,%s,%s,%s,%s,%s);'''
        values = (row['Playlist_ID'], row['Playlist_Title'], row['Playlist_Description'],
                row['Playlist_Publish_Date'],row['Playlist_Channel_ID'], row['Playlist_Channel_Title'],
                row['Playlist_Video_Count'])
        cursor.execute(insert_query, values)
        mydb.commit()
        

# Table creation for videos in postgresql
def create_table_videos(channel_name_single):
    mydb = psycopg2.connect(host = "localhost",user= "postgres",password = "root",database = "Youtube_data_harvesting",port = "5432")
    cursor = mydb.cursor()
   
    create_query = '''create table if not exists videos(video_id varchar(100) primary key,channel_name varchar(100),channel_id varchar(100),
    video_title varchar(100),video_description text,video_publish_date date,video_duration time,video_tags text,video_thumbnail text,
    video_view_count bigint,video_like_count int,video_favorite_count int,video_comment_count int,video_definition varchar(100),
    video_caption varchar(100));'''
    cursor.execute(create_query)
    mydb.commit()
    
    single_video_details =[]
    db = client['Youtube']
    col1 = db['Channel_Details']
    for video_data in col1.find({'Channel_Details.Channel_Name': channel_name_single},{'_id':0,'Video_Details':1}):
        single_video_details.extend(video_data["Video_Details"])
    df_single_video_details = pd.DataFrame(single_video_details)

    def parse_duration(duration):
        try:
            parsed_duration = isodate.parse_duration(duration)  # Parses 'PT43S' to timedelta
            total_seconds = int(parsed_duration.total_seconds())
            formatted_duration = str(timedelta(seconds=total_seconds))  # Converts to 'HH:MM:SS'
            return formatted_duration
        except Exception as e:
            print(f"Error parsing duration: {duration} - {e}")
            return None
        
    df_single_video_details['Video_Duration'] = df_single_video_details['Video_Duration'].apply(parse_duration)

    df_single_video_details['Video_View_Count'] = df_single_video_details['Video_View_Count'].apply(lambda x: min(int(x), 2147483647))

    for index, row in df_single_video_details.iterrows():
        insert_query = '''
        insert into videos(video_id,channel_name,channel_id,video_title,video_description,video_publish_date,
        video_duration,video_tags,video_thumbnail,video_view_count,video_like_count,video_favorite_count,
        video_comment_count,video_definition,video_caption) 
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''

        row['Video_View_Count'] = min(int(row['Video_View_Count']), 2147483647)
        values = (row['Video_ID'], row['Channel_name'], row['Channel_ID'], row['Video_Title'], row['Video_Description'],
                row['Video_Publish_Date'], row['Video_Duration'], row['Video_tags'], row['Video_Thumbnail'],
                row['Video_View_Count'], row['Video_Like_Count'], row['Video_favorite_count'], row['Video_Comment_Count'],
                row['Video_definition'], row['Video_Caption'])
        
        cursor.execute(insert_query, values)
        mydb.commit()
        
        
    
# Table creation for comments in postgresql
def comments_table(channel_name_single):
    # Connect to PostgreSQL database
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="root",
        database="Youtube_data_harvesting",
        port="5432"
    )
    cursor = mydb.cursor()

    create_query = '''
    CREATE TABLE IF NOT EXISTS comments(
        video_id VARCHAR(100),
        video_title VARCHAR(255),
        comment_id VARCHAR(100) PRIMARY KEY,
        comment_text TEXT,
        comment_author VARCHAR(100),
        comment_like_count INT,
        comment_publish_date TIMESTAMP
    );
    '''
    cursor.execute(create_query)
    mydb.commit()

    # Extract comment details from MongoDB
    single_comment_details = []
    db = client['Youtube']
    col1 = db['Channel_Details']

    # Fetch comment details and video title
    for comm_data in col1.find(
        {'Channel_Details.Channel_Name': channel_name_single}, 
        {'_id': 0, 'Comment_Details': 1, 'Video_Details': 1}
    ):
        single_comment_details.extend(comm_data["Comment_Details"])
    
    df_single_comment_details = pd.DataFrame(single_comment_details)

    # Add Video_Title to the DataFrame
    video_details = {}
    for video in col1.find({'Channel_Details.Channel_Name': channel_name_single}, {'_id': 0, 'Video_Details': 1}):
        for vid in video["Video_Details"]:
            video_details[vid["Video_ID"]] = vid["Video_Title"]
    
    df_single_comment_details['Video_Title'] = df_single_comment_details['Video_ID'].map(video_details)

    # Insert data into the comments table
    for index, row in df_single_comment_details.iterrows():
        insert_query = '''
        INSERT INTO comments(
            video_id,
            video_title,
            comment_id,
            comment_text,
            comment_author,
            comment_like_count,
            comment_publish_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s);
        '''
        values = (
            row['Video_ID'],
            row['Video_Title'],
            row['comment_id'],
            row['comment_text'],
            row['comment_author'],
            row['comment_like_count'],
            row['comment_publish_date']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting row {index}: {e}")

    cursor.close()
    mydb.close()



        


def tables(single_channel):
    news = channels_table(single_channel)
    if news:
        return news
    else:
        playlist_table(single_channel)
        create_table_videos(single_channel)
        comments_table(single_channel)
        return 'Tables created successfully'
    
def display_channel_table():
    Ch_list =[]
    db = client['Youtube']
    col1 = db['Channel_Details']
    for ch_data in col1.find({},{'_id':0,'Channel_Details':1}):
        Ch_list.append(ch_data["Channel_Details"])

    df = st.dataframe(Ch_list)
    return df

def display_playlist_table():
    Pl_list =[]
    db = client['Youtube']
    col1 = db['Channel_Details']
    for pl_data in col1.find({},{'_id':0,'Playlist_Details':1}):
        Pl_list.extend(pl_data["Playlist_Details"])

    df1 = st.dataframe(Pl_list)
    return df1

def display_video_table():
  Video_list =[]
  db = client['Youtube']
  col1 = db['Channel_Details']
  for video_data in col1.find({},{'_id':0,'Video_Details':1}):
        Video_list.extend(video_data["Video_Details"])

  df3 = st.dataframe(Video_list)
  return df3

def display_comment_table():
    Comm_list =[]
    db = client['Youtube']
    col1 = db['Channel_Details']
    for comm_data in col1.find({},{'_id':0,'Comment_Details':1}):
        Comm_list.extend(comm_data["Comment_Details"])
        
    df2 = st.dataframe(Comm_list)
    return df2

# # Streamlit code
st.markdown("""<h1 style="font-size:35px;">Youtube Data Harvesting & Warehousing</h1>""",unsafe_allow_html=True,)
st.write("This app harvests data from Youtube channels and stores it in a MongoDB database. It then extracts the data from the MongoDB database and stores it in a PostgreSQL database.")

with st.sidebar:
    st.write("### Instructions")
    st.write("1. Enter the channel ID in the text box.")
    st.write("2. Click the 'Harvest Data' button to start the data harvesting process.")
    st.write("3. Click the 'Create Tables' button to create the tables in the PostgreSQL database.")
    st.write("4. Click the 'Display Tables' button to view the data in the tables.")
    st.write("5. Click the 'Clear Data' button to delete all data from the MongoDB and PostgreSQL databases.")

channel_id = st.text_input("Enter the channel ID:")

# MongoDB connection
client = pymongo.MongoClient("mongodb+srv://Kenny_Ai:kenny_Ai@cluster0.mh35r.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client['Youtube']

# SQL connection
mydb = psycopg2.connect(host = "localhost",user= "postgres",password = "root",database = "Youtube_data_harvesting",port = "5432")
cursor = mydb.cursor()

if st.button("Harvest and store Data"):
    Ch_list =[]
    col1 = db['Channel_Details']
    for ch_data in col1.find({},{'_id':0,'Channel_Details':1}):
        Ch_list.append(ch_data["Channel_Details"]["Channel_ID"])
    if channel_id in Ch_list:
        st.write("Data already exists in the database.")
    else:
        insert = Channel_full_details(channel_id)
        st.success(insert)

All_channels_list =[]
db = client['Youtube']
col1 = db['Channel_Details']
for ch_data in col1.find({},{'_id':0,'Channel_Details':1}):
    All_channels_list.append(ch_data["Channel_Details"]["Channel_Name"])

unique_channel = st.selectbox("Select the channel", All_channels_list)
    
if st.button("Migrate data to PostgreSQL"):
    SQL_tables = tables(unique_channel)
    st.success(SQL_tables)

if st.button("Clear Data"):
    # Drop PostgreSQL tables
    drop_queries = [
        '''drop table if exists comments;''',
        '''drop table if exists videos;''',
        '''drop table if exists playlists;''',
        '''drop table if exists channels;'''
    ]

    for drop_query in drop_queries:
        try:
            cursor.execute(drop_query)
            mydb.commit()
        except Exception as e:
            st.error(f"Error dropping table: {e}")
            mydb.rollback()
    # Drop MongoDB database
    db_name = "Youtube"
    try:
        if db_name in client.list_database_names():
            client.drop_database(db_name)
            st.success(f"Database '{db_name}' has been deleted.")
        else:
            st.info(f"Database '{db_name}' does not exist.")
    except Exception as e:
        st.error(f"Error deleting MongoDB database: {e}")
    # Clear Streamlit session state
    for key in list(st.session_state.keys()):
        del st.session_state[key]

    st.success("All data cleared successfully!")


show_tables = st.radio("Select the table you want to view:", ("Channel", "Playlist", "Video", "Comment"))
if show_tables == "Channel":
    display_channel_table()
elif show_tables == "Playlist":
    display_playlist_table()
elif show_tables == "Video":
    display_video_table()
else:
    display_comment_table()


 
    
    

# SQL connection
mydb = psycopg2.connect(host = "localhost",user= "postgres",password = "root",database = "Youtube_data_harvesting",port = "5432")
cursor = mydb.cursor()

question = st.selectbox("Select your question:", (
                                                  "1. The names of all the videos and their corresponding channels.", 
                                                  "2. Channels that have the most number of videos and their total number of videos.",
                                                  "3. The top 10 most viewed videos and their respective channels.",
                                                  "4. The comments count made on each video and their corresponding video names.",
                                                  "5. The videos which have the highest number of likes and their corresponding channel names.",
                                                  "6. The total number of likes for each video and their corresponding video names.",
                                                  "7. The total number of views for each channel, and what are their corresponding channel names.",
                                                  "8. The names of all the channels that have published videos in the year 2022.",
                                                  "9. The average duration of all videos in each channel and their corresponding channel names.",
                                                  "10. The videos that have the highest number of comments and their corresponding channel names"))

# Sql queries with answers

mydb = psycopg2.connect(host = "localhost",user= "postgres",password = "root",database = "Youtube_data_harvesting",port = "5432")
cursor = mydb.cursor()


if question == "1. The names of all the videos and their corresponding channels":
    query1 = '''select video_title,channel_name from videos;'''
    cursor.execute(query1)
    mydb.commit()
    result1 = cursor.fetchall()
    df = pd.DataFrame(result1, columns = ['Video Title', 'Channel Name'])
    st.write(df)


elif question == "2. Channels that have the most number of videos and their total number of videos.":
    query2 = '''select channel_name,channel_view_count from channels order by channel_view_count desc;'''
    cursor.execute(query2)
    mydb.commit()
    result2 = cursor.fetchall()
    df = pd.DataFrame(result2, columns = ['Channel Name', 'Channel View Count'])
    st.write(df)

elif question == "3. The top 10 most viewed videos and their respective channels.":
    query3 = '''select video_title,video_view_count,channel_name from videos order by video_view_count desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    result3 = cursor.fetchall()
    df = pd.DataFrame(result3, columns = ['Video Title', 'Video View Count','Channel Name'])
    st.write(df)

elif question == "4. The comments count made on each video and their corresponding video names.":
    query4 = '''
    SELECT video_title, COUNT(comment_id) AS number_of_comments
    FROM comments
    GROUP BY video_title
    ORDER BY number_of_comments DESC;
    '''
    cursor.execute(query4)
    result4 = cursor.fetchall()  # No need to commit for SELECT queries
    df = pd.DataFrame(result4, columns=['Video Title', 'Number of Comments'])
    st.write(df)


elif question == "5. The videos which have the highest number of likes and their corresponding channel names.":
    query5 = '''
    SELECT channel_name, video_title, video_like_count
    FROM videos
    ORDER BY video_like_count DESC;
    '''
    cursor.execute(query5)
    result5 = cursor.fetchall()  # No need to commit for SELECT queries
    df = pd.DataFrame(result5, columns=['Channel Name', 'Video Title', 'Video Like Count'])
    st.write(df)


elif question == "6. The total number of likes for each video and their corresponding video names.":
    query6 = '''select video_title,video_like_count from videos;'''
    cursor.execute(query6)
    mydb.commit()
    result6 = cursor.fetchall()
    df = pd.DataFrame(result6, columns = ['Video Title', 'Video Like Count'])
    st.write(df)

elif question == "7. The total number of views for each channel, and what are their corresponding channel names.":
    query7 = '''select channel_name,sum(video_view_count) as total_views from videos group by channel_name;'''
    cursor.execute(query7)
    mydb.commit()
    result7 = cursor.fetchall()
    df = pd.DataFrame(result7, columns = ['Channel Name', 'Total Views'])
    st.write(df)

elif question == "8. The names of all the channels that have published videos in the year 2022.":
    query8 = '''SELECT channel_name, video_title, video_publish_date 
                FROM videos 
                WHERE EXTRACT(year FROM video_publish_date) = 2022;'''
    cursor.execute(query8)
    result8 = cursor.fetchall() 
    df = pd.DataFrame(result8, columns = ['Channel Name', 'Video Title', 'Video Publish Date'])
    st.write(df)


elif question == "9. The average duration of all videos in each channel and their corresponding channel names.":
    query9 = '''SELECT channel_name, 
                       AVG(EXTRACT(epoch FROM video_duration) / 60) AS average_duration_minutes 
                FROM videos 
                GROUP BY channel_name;'''
    cursor.execute(query9)
    result9 = cursor.fetchall()
    df = pd.DataFrame(result9, columns = ['Channel Name', 'Average Duration (Minutes)'])
    st.write(df)

else:
    query10 = '''select video_title as videotitle, channel_name as channelname, video_comment_count as videocommentcount from videos 
                where video_comment_count is not null order by video_comment_count desc;'''
    cursor.execute(query10)
    mydb.commit()
    result10 = cursor.fetchall()
    df = pd.DataFrame(result10, columns =  ['Video Title', 'Channel Name ','Video Comment Count'])
    st.write(df)
