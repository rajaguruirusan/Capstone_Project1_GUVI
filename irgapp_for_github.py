#importing the necessary libraries
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
import time
from googleapiclient.discovery import build
from PIL import Image
from datetime import datetime

# Main Streamlit app
if __name__ == "__main__":
    # Main Streamlit code starts
    # SETTING PAGE CONFIGURATIONS
    icon = Image.open("guvi_logo.png")
    st.set_page_config(
        page_title="Youtube Data Harvesting and Warehousing | By IRG",
        page_icon= icon,
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={'About': """# This app is created by *IRG!*"""}
    )
    st.title("Youtube Data Harvesting and Warehousing | By IRG")
    
    # CREATING OPTION MENU
    with st.sidebar:
        selected = option_menu(None, ["Home","Extract Transform Load - ETL","View"], 
                                icons=["house-door-fill","tools","card-text"],
                                default_index=0,
                                orientation="vertical",
                                styles={"nav-link": {"font-size": "24px", "text-align": "centre", "margin": "0px", 
                                                        "--hover-color": "#009933"},
                                        "icon": {"font-size": "24px"},
                                        "container" : {"max-width": "7000px"},
                                        "nav-link-selected": {"background-color": "#009933"}})
    # Bridging a connection with MongoDB Atlas and Creating a new database (youtube_data)
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['youtube_data']
    
    # Connection parameters
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'xxxxx', ## Write MySQL Password
    }

    # Establish the connection and create a cursor
    mydb = sql.connect(**config)
    mycursor = mydb.cursor(buffered=True)

    # Create the 'youtube_db' database if it doesn't exist
    mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")

    # Commit the changes and close the connection
    mydb.commit()
    mydb.close()
    # Now you can connect to the 'youtube_db' database
    config['database'] = 'youtube_db'
    mydb = sql.connect(**config)
    mycursor = mydb.cursor(buffered=True)

    # Create the 'channels' table if it doesn't exist
    mycursor.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id INT AUTO_INCREMENT PRIMARY KEY,
            channel_id VARCHAR(255),
            channel_name VARCHAR(255),
            playlist_id VARCHAR(255),
            subscribers INT,
            views INT,
            total_videos INT,
            description TEXT,
            country VARCHAR(255)
            -- Add other columns as needed
        )
    """)

    # Create the 'videos' table if it doesn't exist
    mycursor.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            channel_name VARCHAR(255),
            channel_id VARCHAR(255),
            video_id VARCHAR(255),
            title VARCHAR(255),
            tags VARCHAR(512),
            thumbnail VARCHAR(255),
            description TEXT,
            published_date DATETIME,
            duration VARCHAR(255),
            views INT,
            likes INT,
            comments INT,
            favorite_count INT,
            definition VARCHAR(255),
            caption_status VARCHAR(255)
        )
    """)

    # Create the 'comments' table if it doesn't exist
    mycursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            channel_name VARCHAR(255),
            comment_id VARCHAR(255),
            video_id VARCHAR(255),
            comment_text TEXT,
            comment_author VARCHAR(255),
            comment_posted_date DATETIME,
            like_count INT,
            reply_count INT
        )
    """)

    # Commit the changes and close the connection
    mydb.commit()

    # BUILDING CONNECTION WITH YOUTUBE API
    api_key = "xxxxx" ##Enter the API key generated from Google Developer, Refer url https://developers.google.com/youtube/v3/getting-started
    youtube = build('youtube','v3',developerKey=api_key)


    # FUNCTION TO GET CHANNEL DETAILS
    def get_channel_details(channel_id):
        ch_data = []
        response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                        id= channel_id).execute()

        for i in range(len(response['items'])):
            data = dict(Channel_id = channel_id[i],
                        Channel_name = response['items'][i]['snippet']['title'],
                        Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                        Subscribers = response['items'][i]['statistics']['subscriberCount'],
                        Views = response['items'][i]['statistics']['viewCount'],
                        Total_videos = response['items'][i]['statistics']['videoCount'],
                        Description = response['items'][i]['snippet']['description'],
                        Country = response['items'][i]['snippet'].get('country')
                        )
            ch_data.append(data)
        return ch_data


    # FUNCTION TO GET VIDEO IDS
    def get_channel_videos(channel_id):
        video_ids = []
        # get Uploads playlist id
        res = youtube.channels().list(id=channel_id, 
                                    part='contentDetails').execute()
        playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token = None
        
        while True:
            res = youtube.playlistItems().list(playlistId=playlist_id, 
                                            part='snippet', 
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
            
            for i in range(len(res['items'])):
                video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token = res.get('nextPageToken')
            
            if next_page_token is None:
                break
        return video_ids


    # FUNCTION TO GET VIDEO DETAILS
    def get_video_details(v_ids):
        video_stats = []
        
        for i in range(0, len(v_ids), 50):
            response = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=','.join(v_ids[i:i+50])).execute()
            for video in response['items']:
                video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                    Channel_id = video['snippet']['channelId'],
                                    Video_id = video['id'],
                                    Title = video['snippet']['title'],
                                    Tags = video['snippet'].get('tags'),
                                    Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                    Description = video['snippet']['description'],
                                    Published_date = video['snippet']['publishedAt'],
                                    Duration = video['contentDetails']['duration'],
                                    Views = video['statistics']['viewCount'],
                                    Likes = video['statistics'].get('likeCount'),
                                    Comments = video['statistics'].get('commentCount'),
                                    Favorite_count = video['statistics']['favoriteCount'],
                                    Definition = video['contentDetails']['definition'],
                                    Caption_status = video['contentDetails']['caption']
                                )
                video_stats.append(video_details)
        return video_stats


    # FUNCTION TO GET COMMENT DETAILS
    def get_comments_details(v_id):
        comment_data = []
        try:
            next_page_token = None
            while True:
                response = youtube.commentThreads().list(part="snippet,replies",
                                                        videoId=v_id,
                                                        maxResults=100,
                                                        pageToken=next_page_token).execute()
                for cmt in response['items']:
                    data = dict(Comment_id = cmt['id'],
                                Video_id = cmt['snippet']['videoId'],
                                Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                                Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                                Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                                Reply_count = cmt['snippet']['totalReplyCount']
                            )
                    comment_data.append(data)
                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break
        except:
            pass
        return comment_data


    # FUNCTION TO GET CHANNEL NAMES FROM MONGODB
    def channel_names():
        # To fetch channel names from MongoDB
        collections = db.channel_details
        mongo_channel_names = [i['Channel_name'] for i in collections.find({}, {'Channel_name': 1, '_id': 0})]

        # Fetch channel names from SQL
        mycursor.execute("SELECT channel_name FROM channels")
        sql_channel_names = [result[0] for result in mycursor.fetchall()]

        # Exclude SQL channel names from MongoDB channel names
        remaining_channel_names = list(set(mongo_channel_names) - set(sql_channel_names))

        return remaining_channel_names

    # HOME PAGE
    if selected == "Home":
        # Title Image
        
        col1,col2 = st.columns(2,gap= 'medium')
        col1.markdown("## :blue[Domain] : Social Media")
        col1.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
        col1.markdown("## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
        col2.markdown("#   ")
        col2.markdown("#   ")
        col2.markdown("#   ")
        col2.image("guvi_logo.png")
    # EXTRACT TRANSFORM LOAD PAGE
    if selected == "Extract Transform Load - ETL":
        tab1, tab2, tab3 = st.tabs([r"$\Large EXTRACT $", r"$\Large TRANSFORM $",r"$\Large LOAD $"])
        
        # Define function for MongoDB upload activity
        ch_details = []
        def upload_to_mongodb(ch_id):
            with st.spinner('Transferring Data to MongoDB'):
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)
                
                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d += get_comments_details(i)
                    return com_d
                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                # Check if the list is not empty before calling insert_many
                if comm_details:
                    collections3.insert_many(comm_details)
                else:
                    # Insert a document with a field set to null
                    null_document = {"Comment_id": None} 
                    collections3.insert_one(null_document)
                st.success("YouTube Data Transfer to MongoDB is successful !!")
            st.table(ch_details)

        # EXTRACT TAB
        with tab1:
            st.markdown("#    ")
            st.write("### Enter YouTube Channel ID below :")
            ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channelId").split(',')
            
            if ch_id and st.button("Extract Data"):
                ch_details = get_channel_details(ch_id)
                st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
                st.table(ch_details)
        
        with tab2:
            st.markdown("### Trasfer YouTube Data Extract to MongoDB")
            if ch_id:
                st.table(ch_details)  # Display channel table above the button
                if st.button("Transfer to MongoDB"):
                    upload_to_mongodb(ch_id)        
        
        # LOAD TAB
        with tab3:     
            st.markdown("#   ")
            st.markdown("### Select a channel to Load YouTube Data to SQL")
        
            ch_names = channel_names()
            user_inp = st.selectbox("Select channel", options=ch_names)
        
            def channel_already_transformed():
                mycursor.execute("SELECT COUNT(*) FROM channels WHERE channel_name = %s", (user_inp,))
                result = mycursor.fetchone()
                return result[0] > 0

            def insert_into_channels():
                collections = db.channel_details
                query = """
                    INSERT INTO channels 
                    (channel_id, channel_name, playlist_id, subscribers, views, total_videos, description, country)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                for i in collections.find({"Channel_name": user_inp}, {'_id': 0}):
                    values = (
                        i['Channel_id'],
                        i['Channel_name'],
                        i['Playlist_id'],
                        i['Subscribers'],
                        i['Views'],
                        i['Total_videos'],
                        i['Description'],
                        i['Country']
                    )
                    
                    mycursor.execute(query, values)
                    mydb.commit()

            def video_already_transformed():
                mycursor.execute("SELECT COUNT(*) FROM videos WHERE channel_name = %s", (user_inp,))
                result = mycursor.fetchone()
                return result[0] > 0

            def insert_into_videos():
                collectionss = db.video_details
                query1 = """
                    INSERT INTO videos (channel_name, channel_id, video_id, title, tags, thumbnail, description, published_date, duration, views, likes, comments, favorite_count, definition, caption_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                for i in collectionss.find({"Channel_name": user_inp}, {"_id": 0}):
                    # Check if 'Tags' exists and is not None
                    tags_list = i.get('Tags')
                    if tags_list is not None:
                        # Convert 'Tags' list to a comma-separated string and limit the length to 512 characters
                        tags_str = ', '.join(map(str, tags_list))[:512]
                    else:
                        # If 'Tags' is None, set an empty string
                        tags_str = ''

                    # Convert 'published_date' to MySQL-compatible format
                    published_date_str = i['Published_date']
                    published_date_dt = datetime.fromisoformat(published_date_str.replace("Z", "+00:00"))

                    values = (
                        i['Channel_name'],
                        i['Channel_id'],
                        i['Video_id'],
                        i['Title'],
                        tags_str,
                        i['Thumbnail'],
                        i['Description'],
                        published_date_dt,
                        i['Duration'],
                        i['Views'],
                        i['Likes'],
                        i['Comments'],
                        i['Favorite_count'],
                        i['Definition'],
                        i['Caption_status']
                    )
                    mycursor.execute(query1, values)
                    mydb.commit()

            def comments_already_transformed():
                mycursor.execute("SELECT COUNT(*) FROM comments WHERE channel_name = %s", (user_inp,))
                result = mycursor.fetchone()
                return result[0] > 0

            def insert_into_comments():
                collections1 = db.video_details
                collections2 = db.comments_details
                query2 = """
                    INSERT INTO comments 
                    (comment_id, video_id, comment_text, comment_author, comment_posted_date, like_count, reply_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                for vid in collections1.find({"Channel_name": user_inp}, {'_id': 0}):
                    for i in collections2.find({'Video_id': vid['Video_id']}, {'_id': 0}):
                        # Convert 'comment_posted_date' to MySQL-compatible format
                        comment_posted_date_str = i['Comment_posted_date']
                        comment_posted_date_dt = datetime.fromisoformat(comment_posted_date_str.replace("Z", "+00:00"))

                        t = (
                            i['Comment_id'],
                            i['Video_id'],
                            i['Comment_text'],
                            i['Comment_author'],
                            comment_posted_date_dt,
                            i['Like_count'],
                            i['Reply_count']
                        )

                        mycursor.execute(query2, t)
                    mydb.commit()

            if st.button("Load to SQL"):
                # Add spinner
                with st.spinner("Please wait, YouTube data is loading to MySQL..."):
                    try:
                        # Reconnect to MySQL database
                        mydb = sql.connect(**config)
                        mycursor = mydb.cursor(buffered=True)

                        if not channel_already_transformed():
                            insert_into_channels()

                        if not video_already_transformed():
                            insert_into_videos()

                        if not comments_already_transformed():
                            insert_into_comments()

                        st.success("YouTube Data loaded to MySQL Successfully!!!")
                    except Exception as e:
                        st.error(f"An error occurred: {str(e)}")
                        import traceback
                        st.text(traceback.format_exc())
                    finally:
                        # Close the connection after transformation
                        mydb.close()
            
    # VIEW PAGE
    if selected == "View":
        
        st.write("## :orange[Select any question to get Insights]")
        questions = st.selectbox('Questions',
        ['Click the question that you would like to query',
        '1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
        
        if questions == '1. What are the names of all the videos and their corresponding channels?':
            mycursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name""")
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            df.index = df.index + 1
            st.write(df)
            
        elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            mycursor.execute("""SELECT channel_name 
            AS Channel_Name, total_videos AS Total_Videos
                                FROM channels
                                ORDER BY total_videos DESC""")
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            df.index = df.index + 1
            st.write(df)
            st.write("### :green[Number of videos in each channel :]")
            #st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
            fig = px.bar(df,
                        x=mycursor.column_names[0],
                        y=mycursor.column_names[1],
                        orientation='v',
                        color=mycursor.column_names[0]
                        )
            st.plotly_chart(fig,use_container_width=True)
            
        elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                                FROM videos
                                ORDER BY views DESC
                                LIMIT 10""")
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            df.index = df.index + 1
            st.write(df)
            st.write("### :green[Top 10 most viewed videos :]")
            fig = px.bar(df,
                        x=mycursor.column_names[2],
                        y=mycursor.column_names[1],
                        orientation='h',
                        color=mycursor.column_names[0]
                        )
            st.plotly_chart(fig,use_container_width=True)
            
        elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
            mycursor.execute("""SELECT a.channel_name AS Channel_Name, a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                                FROM videos AS a
                                LEFT JOIN (SELECT video_id, COUNT(comment_id) AS Total_Comments
                                        FROM comments GROUP BY video_id) AS b
                                ON a.video_id = b.video_id
                                ORDER BY b.Total_Comments DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            df.index = df.index + 1
            st.write(df)

            
        elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                                FROM videos
                                ORDER BY likes DESC
                                LIMIT 10""")
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            df.index = df.index + 1
            st.write(df)
            st.write("### :green[Top 10 most liked videos :]")
            fig = px.bar(df,
                        x=mycursor.column_names[2],
                        y=mycursor.column_names[1],
                        orientation='h',
                        color=mycursor.column_names[0]
                        )
            st.plotly_chart(fig,use_container_width=True)
            
        elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Title, likes AS Likes_Count
                                FROM videos
                                ORDER BY likes DESC""")
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            df.index = df.index + 1
            st.write("##### Note: Dislike count was removed by YouTube, Hence counted Likes only")
            st.write(df)
            
        elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                                FROM channels
                                ORDER BY views DESC""")
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)
            st.write("### :green[Channels vs Views :]")
            fig = px.bar(df,
                        x=mycursor.column_names[0],
                        y=mycursor.column_names[1],
                        orientation='v',
                        color=mycursor.column_names[0]
                        )
            st.plotly_chart(fig,use_container_width=True)
            
        elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
            mycursor.execute("""SELECT channel_name AS Channel_Name
                                FROM videos
                                WHERE published_date LIKE '2022%'
                                GROUP BY channel_name
                                ORDER BY channel_name""")
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            df.index = df.index + 1
            st.write(df)
            
        elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name, 
                            ROUND(SUM(duration_sec) / COUNT(*), 0) AS average_duration
                            FROM (
                                SELECT channel_name, 
                                CASE
                                    WHEN duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                                    TIME_TO_SEC(CONCAT(
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'H', 1), 'T', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'H', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                                ))
                                    WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                                    TIME_TO_SEC(CONCAT(
                                    '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'T', -1), ':',
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                                ))
                                    WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                                    TIME_TO_SEC(CONCAT('0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'T', -1)))
                                    END AS duration_sec
                            FROM videos
                            ) AS subquery
                            GROUP BY channel_name
                            ORDER BY average_duration DESC""")
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            df.index = df.index + 1
            st.write(df)
            st.write("### :green[Average video duration for channels :]")
            fig = px.bar(df,
                        x=mycursor.column_names[0],
                        y=mycursor.column_names[1],
                        orientation='v',
                        color=mycursor.column_names[0]
                        )
            st.plotly_chart(fig,use_container_width=True)


            
        elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name,title AS 'Video Title',video_id AS Video_ID,comments AS Comments
                                FROM videos
                                ORDER BY comments DESC
                                LIMIT 10""")
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            df.index = df.index + 1
            st.write(df)
            st.write("### :green[Videos with most comments :]")
            fig = px.bar(df,
                        x=mycursor.column_names[1],
                        y=mycursor.column_names[3],
                        orientation='v',
                        color=mycursor.column_names[0]
                        )
            st.plotly_chart(fig,use_container_width=True)
    
    # Using st.cache_data for a time-consuming function
    @st.cache_data
    def example_time_consuming_function():
        # Implementation of the time-consuming function...
        return "Result of the time-consuming function"

    # Automatically runs and caches the function
    result = example_time_consuming_function()
    # st.write("Result:", result)  # Commenting out this line to avoid displaying the result