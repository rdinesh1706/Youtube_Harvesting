from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
import statistics

# connecting the api key


def Api_connection():
    Api_id = "AIzaSyDHH42n_-fjgFUZP6_DP7AVOi_owtsSxyA"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=Api_id)

    return youtube


youtube = Api_connection()


# getting the channel required information

def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response['items']:  # looping the information that we want
        data = dict(Channel_Name=i['snippet']['title'],
                    Channel_Id=i['id'],
                    Subscriber=i['statistics']['subscriberCount'],
                    Views=i['statistics']['viewCount'],
                    Total_Videos=i['statistics']['videoCount'],
                    Channel_Description=i['snippet']['description'],
                    Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data


# getting video id's
def get_videos_ids(channel_id):
    video_ids = []

    response = youtube.channels().list(id=channel_id,
                                       part='contentDetails').execute()

    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:  # this is because to run the loop till the end
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]
                             ['snippet']['resourceId']['videoId'])
    # response1['pageInfo']['totalResults']
        # get is used for if data is there fetch it
        next_page_token = response1.get('nextPageToken')
        # nextPageToken is used to get 50's video data

        if next_page_token is None:
            break
    return video_ids

# using videoId we are fetching the video details
# getting video id's infromation


def get_video_info(video_ids):
    video_data = []
    # PPublished_Date = None
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Description=item['snippet'].get('description'),
                        Published_Date=(item['snippet']['publishedAt']).replace(
                            "T", " ").replace("Z", ""),
                        Views=item['statistics'].get('viewCount'),
                        Likes=item['statistics'].get('likeCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Comments=item['statistics'].get('commentCount'),
                        Duration=item['contentDetails']['duration'],
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Caption_Status=item['contentDetails']['caption'],
                        Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Definition=item['contentDetails']['definition'],
                        )
            video_data.append(data)

    return video_data


# using videoId comments
def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=30  # limiting the comments
            )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=(item['snippet']['topLevelComment']['snippet']['publishedAt']).replace(
                                "T", " ").replace("Z", "")
                            )
                Comment_data.append(data)
    except:
        pass
    return Comment_data

# fetching playlist details


def get_playlist_details(channel_id):
    next_page_token = None
    All_data = []
    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        PublishedAt=(item['snippet']['publishedAt']).replace(
                            "T", " ").replace("Z", ""),
                        Video_Count=item['contentDetails']['itemCount']
                        )
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data


# connecting and uploading data in mongodb
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["youtube_data"]


def channel_details(channel_id):
    chnl_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vid_details = get_videos_ids(channel_id)
    vifo_details = get_video_info(vid_details)
    cmt_details = get_comment_info(vid_details)

    collect1 = db["channel_details"]
    collect1.insert_one(
        {
            "channel_information": chnl_details,
            "playlist_information": pl_details,
            "video_information": vifo_details,
            "comment_information": cmt_details
        }
    )

    return "uploded"

# table  creation


def channels_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="rdx@17",
        database="youtube_data"
    )
    cursor = mydb.cursor()

    # drop_query = '''drop table if exists channels'''
    # cursor.execute(drop_query)
    # mydb.commit()

    try:
        create_query = '''create table channels(
                                                                Channel_Name varchar(100),
                                                                Channel_Id varchar(100) primary key,
                                                                Subscriber bigint,
                                                                Views bigint,
                                                                Total_Videos int,
                                                                Channel_Description text,
                                                                Playlist_Id varchar(100)
        )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("table created")

    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscriber,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id
                                                )
                                                values(%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Subscriber'],
                  row['Views'],
                  row['Total_Videos'],
                  row['Channel_Description'],
                  row['Playlist_Id'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("channel already inserted")

# playlist table creation


def playlist_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="rdx@17",
        database="youtube_data"
    )
    cursor = mydb.cursor()

    # drop_query = '''drop table if exists playlists'''
    # cursor.execute(drop_query)
    # mydb.commit()
    try:
        create_query = '''create table playlists(
                                                                Playlist_Id varchar(100) primary key,
                                                                Title varchar(100),
                                                                Channel_Id varchar(100),
                                                                Channel_Name varchar(100),
                                                                PublishedAt timestamp,
                                                                Video_Count int
                                                                )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("already")
    pl_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        insert_query = '''insert into playlists( Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count
                                                )
                                                values(%s,%s,%s,%s,%s,%s)'''

        values = (row['Playlist_Id'],
                  row['Title'],
                  row['Channel_Id'],
                  row['Channel_Name'],
                  row['PublishedAt'],
                  row['Video_Count'])
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("already")

# video table creation


def videos_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="rdx@17",
        database="youtube_data"
    )
    cursor = mydb.cursor()

    # drop_query = '''drop table if exists videos'''
    # cursor.execute(drop_query)
    # mydb.commit()
    try:
        create_query = '''create table videos(
                                                            Video_Id varchar(100) primary key,
                                                            Title varchar(100),
                                                            Description text,
                                                            Published_Date timestamp,
                                                            Views bigint,
                                                            Likes bigint,
                                                            Favorite_Count int,
                                                            Comments int,
                                                            Duration varchar(100),
                                                            Thumbnail varchar(100),
                                                            Caption_Status varchar(100),
                                                            Channel_Name varchar(100),
                                                            Channel_Id varchar(100),
                                                            Definition varchar(100)
                                                                )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("already")

    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        insert_query = '''insert into videos(    
                                                        Video_Id,
                                                        Title,
                                                        Description,
                                                        Published_Date,
                                                        Views,
                                                        Likes,
                                                        Favorite_Count,
                                                        Comments,
                                                        Duration,
                                                        Thumbnail,
                                                        Caption_Status,
                                                        Channel_Name,
                                                        Channel_Id,
                                                        Definition
                                                    )
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['Video_Id'],
                  row['Title'],
                  row['Description'],
                  row['Published_Date'],
                  row['Views'],
                  row['Likes'],
                  row['Favorite_Count'],
                  row['Comments'],
                  row['Duration'],
                  row['Thumbnail'],
                  row['Caption_Status'],
                  row['Channel_Name'],
                  row['Channel_Id'],
                  row['Definition']
                  )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("already")

# comments table creation


def comments_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="rdx@17",
        database="youtube_data"
    )
    cursor = mydb.cursor()

    # drop_query = '''drop table if exists comments'''
    # cursor.execute(drop_query)
    # mydb.commit()
    try:
        create_query = '''create table comments(
                                                            Comment_Id varchar(100) primary key,
                                                            Video_Id varchar(100),
                                                            Comment_Text text,
                                                            Comment_Author varchar(100),
                                                            Comment_Published timestamp
                                                                )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("already")

    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)

    for index, row in df3.iterrows():
        insert_query = '''insert into comments(    
                                                        Comment_Id,
                                                        Video_Id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published
                                                        )
                                                        values(%s,%s,%s,%s,%s)'''

        values = (row['Comment_Id'],
                  row['Video_Id'],
                  row['Comment_Text'],
                  row['Comment_Author'],
                  row['Comment_Published'],
                  )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("already")


def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()
    return "tables created"


def show_channels_table():
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)

    return df


def show_playlist_table():
    pl_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)

    return df1


def show_videos_table():
    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)

    return df2


def show_comments_table():
    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)

    return df3

# streamlit part

# using slidebar to display all the technologies and skills used in this project


with st.sidebar:
    st.markdown("<h1 style='text-align:center;font-family:Georgia;background-color: #DCD494;border: 2px solid red;border-radius: 25px;,'>Youtube Harvesting Project🫵🧪</h1>",
                unsafe_allow_html=True)

    # st.title("Press the table for the view 👇")
    show_table = st.radio(
        "Press the table for the view 👇",
        ["CHANNELS📺", "PLAYLISTS⏯▶", "VIDEOS📽📸", "COMMENTS📃📄"],
        captions=["To view the channels.", "To view the Playlist.", "To view the Videos.", "To view the comments"])


channel_id = st.text_input("Enter the channel ID Here")


if st.button("collect"):
    ch_ids = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details are already exists")
    else:
        insert = channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to Sql"):
    Table = tables()
    st.success(Table)


# displaying the details if they press the particular segment
if show_table == "CHANNELS📺":
    show_channels_table()

elif show_table == "PLAYLISTS⏯▶":
    show_playlist_table()

elif show_table == "VIDEOS📽📸":
    show_videos_table()

elif show_table == "COMMENTS📃📄":
    show_comments_table()


# answering all the questions using queries

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="rdx@17",
    database="youtube_data"
)
cursor = mydb.cursor(buffered=True)

# Details are displayed by clicking the question in the dropdown list box and those questions are

question = st.selectbox("Choose the Question", ("1. All the videos and the channel name",
                                                "2. Channels with most number of videos",
                                                "3. Ten most viewed videos",
                                                "4. Comments in each videos",
                                                "5. Videos with highest likes",
                                                "6. Likes of all videos",
                                                "7. Views of each channel",
                                                "8. Videos published in the year of 2022",
                                                "9. Average duration of all videos in each channel",
                                                "10. Videos with Highest number of comments"))

if question == "1. All the videos and the channel name":
    query1 = ''' select title as videos, channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns=["video_title", "channel name"])
    st.write(df)
elif question == "2. Channels with most number of videos":
    query2 = ''' select channel_name as channelname, total_videos as no_video from channels order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name", "no of videos"])
    st.write(df2)
elif question == "3. Ten most viewed videos":
    query3 = '''select views as views, channel_name as channelName, title as title from videos order by views desc '''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["high views", "channel name", "title"])
    st.write(df3)
elif question == "4. Comments in each videos":
    query4 = '''select comments as comments, title as videotitle from videos where comments is not null  '''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["comments count", "title"])
    st.write(df4)
elif question == "5. Videos with highest likes":
    query5 = '''select channel_name as channel_name ,likes as likes,title as title from videos order by likes desc  '''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["channel name", "likes", "video title"])
    st.write(df5)
elif question == "6. Likes of all videos":
    query6 = '''select likes as likes,title as title from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["likes", "video title"])
    st.write(df6)
elif question == "7. Views of each channel":
    query7 = '''select views as views, channel_name as channel_name from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["views", "channel name"])
    st.write(df7)
elif question == "8. Videos published in the year of 2022":
    query8 = '''select title as title, published_date as published_date, channel_name as channel_name from videos 
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(
        t8, columns=["video title", "published year", "channel name"])
    st.write(df8)
elif question == "9. Average duration of all videos in each channel":
    query9 = '''select channel_name as channel_name, duration as duration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channelname", "duration"])
    T9 = []
    for index, row in df9.iterrows():
        channel_name = row["channelname"]
        average_duration = row["duration"]
        aveg_duration = average_duration.replace(
            "PT", "").replace("M", "").replace("S", "")
        avg_int = int(aveg_duration)
        T9.append(dict(channel_name=channel_name, avgduration=avg_int))
    df11 = pd.DataFrame(T9)
    st.write(df11)
elif question == "10. Videos with Highest number of comments":
    query10 = '''select title as videotitle, channel_name as channelname, comments as comments from videos 
                where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(
        t10, columns=["video title", "channelname", "comments"])
    st.write(df10)
