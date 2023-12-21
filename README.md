# Youtube_Harvesting

Welcome to my Github Youtube data harveting repository here I done a project in streamlit application

The concept of this project is to collect and tabulate the youtube details to fetch from those details as a required format of report by using youtubeapi

Project Title: Youtube data harvesting and Warehousing using SQL, MongoDB Compass and Streamlit

Skills used: Python Scripting, Data Collection, MongoDB, Streamlit, API Integration, Data Management using MongoDB(Compass) and SQL

Project Process Explaination:

1) Connecting the API Key

2) Getting the Channel required Details - def get_channel_info(channel_id):

3) Getting the channels video Id's to fetch the each video details - def get_videos_ids(channel_id):

4) Getting video's data from each videoids - def get_video_info(video_ids):

5) Fetching comments details from videoids - def get_comment_info(video_ids):

6) Fectching playlist details from channelids - def get_playlist_details(channel_id):

7) Connecting the MongoDb and creating a database and inserting collected data

8) Creating a function to do all the collecting function scripts to run

9) Creating a function to transfer all data from mongoDB to MySql
	a)channel table - def channels_table():
	b)playlist table - def playlist_table():
	c)video table - def videos_table():
	d)comments table - def comments_table():

10) def tables(): - is a function to run all the tables details and transfering details by calling this function

11) To show all the details if the press the particular segment
	a)channel table - def show_channels_table():
	b)playlist table - def show_playlist_table():
	c)video table - def show_videos_table():
	d)comments table - def show_comments_table():

12) In Streamlit using slidebar to display all the technologies and skills used in this project

13) Displaying the details if they press the particular segment in radio button

14) Details are displayed by clicking the question in the dropdown list box and those questions are
						"1. All the videos and the channel name",
                                                "2. Channels with most number of views",
                                                "3. Ten most viewed videos",
                                                "4. Comments in each videos",
                                                "5. Videos with highest likes",
                                                "6. Likes of all videos",
                                                "7. Views of each channel",
                                                "8. Videos published in the year of 2022",
                                                "9. Average duration of all videos in each channel",
                                                "10. Videos with Highest number of comments"

15) Displaying those questions details using queries by fetching those details from sql individually


