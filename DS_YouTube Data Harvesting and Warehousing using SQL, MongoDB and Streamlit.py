#!/usr/bin/env python
# coding: utf-8

# In[1]:


import streamlit as st
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import pandas as pd
import pymysql
from pprint import pprint
import datetime
import re


# In[2]:


# Connect to MongoDB
client = MongoClient("mongodb+srv://RaghuveerHosamaniR:Raghuveer9964@cluster0.wtwz60c.mongodb.net/")
db = client["Youtube_Data"]
collection = db["channel_stats"]


# In[3]:


con = pymysql.connect(host = 'localhost',
                        user = 'root',
                        password = "Raghuveer9964@"
                      )


# In[4]:


mycursor = con.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS YOUTUBEHARVEST1")
mycursor.execute("USE YOUTUBEHARVEST1")


# In[5]:


api_key="AIzaSyBlFgPHuT23d7p5FocmnN8hW5U9PT5K57I"
channel_id=['UCr-gTfI7au9UaEjNCbnp_Nw',# vidy vox
            'UC1mupr-2YbkxQVmcO3ve6SA',#Anirudh
            'UC3mb5QRlm4VQmOZD_P0ctGw',#AR Rehaman
            'UCcL78rRNuUQ8t7Dx4CLmRqA',#shreya Goghal
           'UCtFOW7jJXChfFNoucRFqRmw',#Arjith singh
           'UCrYczeBh8tcxLLAgn8m2wWQ',# udith narayan
           'UCDYFISYJx2tSc6cyhvx0N5Q',#sonu nigam
           'UCLx-YFOk_NgXNG7uCXq8m5w',#ritviz
           'UC1KonH1j8WMhc5cT6Bp7NtA',#Honey singh
           'UC1GBYS8_8cXRDM3yOYHeyWw',#Arman mallik
            ]
youtube=build("youtube",'v3', developerKey=api_key)


# In[6]:


def get_channel_stats(youtube, channel_id):
    all_data = []
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute() 

    for i in range(len(response['items'])):
        data = dict(Channel_name = response['items'][i]['snippet']['title'],
            Channel_id = response['items'][i]['id'],
            Subscribers_Count = int(response['items'][i]['statistics']['subscriberCount']),
            Views = int(response['items'][i]['statistics']['viewCount']),
            Total_videos = int(response['items'][i]['statistics']['videoCount']),
            Channel_Description = response['items'][i]['snippet']['description'],
            playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        all_data.append(data)

    return all_data


# In[7]:


get_channel_stats(youtube, channel_id)


# In[8]:


def get_video_ids(youtube,channel_id):
    video_ids = []
    request = youtube.channels().list(
                                        part="contentDetails",
                                        id=channel_id)
    response = request.execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        response = youtube.playlistItems().list(playlistId=playlist_id,
                                        part = 'snippet',
                                        maxResults = 50,
                                        pageToken = next_page_token).execute()
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids  


# In[9]:


video_ids=get_video_ids(youtube,channel_id)
video_ids


# In[10]:


def convert_duration(duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(regex, duration)
    if not match:
        return '00:00:00'
    hours,minutes,seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    total_seconds = hours*3600+minutes*60*seconds
    return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds/3600),int((total_seconds%3600)/60),int(total_seconds%60))


# In[11]:


def get_video_details(youtube, video_ids):
    all_video_stats = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
                                        part='snippet,statistics,contentDetails',
                                        id=','.join(video_ids[i:i+50]))
        response = request.execute()
        

        for j in range(len(response['items'])):
            video_stats = dict(channel_name = response['items'][j]['snippet']['channelTitle'],
                        channel_id = response['items'][j]['snippet']['channelId'],
                        Title = response['items'][j]['snippet']['title'],
                        videoid = response['items'][j]['id'],
                        Thumbnail = response['items'][j]['snippet']['thumbnails']['default']['url'],
                        Favourite_count = int(response['items'][j]['statistics']['favoriteCount']),
                        description = response['items'][j]['snippet']['description'],
                        duration = convert_duration(response['items'][j]['contentDetails']['duration']),
                        Published_date =str(datetime.datetime.strptime(response['items'][j]['snippet']['publishedAt'] ,"%Y-%m-%dT%H:%M:%SZ")),
                        Views =int(response['items'][j]['statistics']['viewCount']),
                        Likes =int(response['items'][j]['statistics']['likeCount']),
                        Comments_count = int(response['items'][j]['statistics']['commentCount']),
                        Definition = response['items'][j]['contentDetails']['definition'],
                        Caption_status = response['items'][j]['contentDetails']['caption']
                        )
            all_video_stats.append(video_stats)

    return all_video_stats


# In[12]:


get_video_details(youtube, video_ids)


# In[13]:


def comment_details(youtube,video_ids):
    comments = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(part="snippet,replies",videoId=video_id)
            response=request.execute()
            if 'items' not in response:
                continue
            for i in response['items']:
                comment_information = {'comment_Id': i["id"],
                            'comment_Author': i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            'comment_Text': i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],                                      'comment_PublishedAt':str(datetime.datetime.strptime( i["snippet"]["topLevelComment"]["snippet"]["publishedAt"],"%Y-%m-%dT%H:%M:%SZ")),
                            'Video_Id': i["snippet"]["topLevelComment"]["snippet"]["videoId"],
                            'comment_ch_id': i["snippet"]["topLevelComment"]["snippet"]["authorChannelId"]["value"],
                            'comment_like_count':int(i["snippet"]["topLevelComment"]["snippet"]["likeCount"]),
                            'comment_Reply_count': int(i["snippet"]["totalReplyCount"])
                            }
                comments.append(comment_information)
        except:
            pass
    return comments


# In[14]:


comment_details(youtube,video_ids)


# In[15]:


selected = option_menu(
    menu_title ="YOUTUBE DATA HARVESTING AND WAREHOUSING",
    options = ["DATA COLLECTION","SELECT AND STORE","MIGRATION OF DATA","DATA ANALYSIS"],
    icons = ["cloud-upload-fill","filetype-sql","search"],
    menu_icon = "cast",
    default_index=0,
    orientation="horizontal"
)

if selected == "DATA COLLECTION":
    st.title(f"DATA COLLECTION PAGE")
    df = pd.DataFrame(
        {"Channel_Name" :["vidy vox","Anirudh","AR Rehaman","shreya Goghal","Arjith singh","udith narayan","sonu nigam",
                         "sonu nigam","ritviz","Honey singh,Arman mallik"],
        "Channel_ID" :['UCr-gTfI7au9UaEjNCbnp_Nw',# vidy vox
            'UC1mupr-2YbkxQVmcO3ve6SA',#Anirudh
            'UC3mb5QRlm4VQmOZD_P0ctGw',#AR Rehaman
            'UCcL78rRNuUQ8t7Dx4CLmRqA',#shreya Goghal
           'UCtFOW7jJXChfFNoucRFqRmw',#Arjith singh
           'UCrYczeBh8tcxLLAgn8m2wWQ',# udith narayan
           'UCDYFISYJx2tSc6cyhvx0N5Q',#sonu nigam
           'UCLx-YFOk_NgXNG7uCXq8m5w',#ritviz
           'UC1KonH1j8WMhc5cT6Bp7NtA',#Honey singh
           'UC1GBYS8_8cXRDM3yOYHeyWw'#Arman mallik
                      ]
        }
    )
    st.data_editor(df)

if selected == "SELECT AND STORE":
    title = st.text_input('Enter a channel id')       
    channel_id = title     
    clicked = st.button("Extract") 
    if clicked == True:
        def main(channel_id):
            c=get_channel_stats(youtube, channel_id)
            p=get_playlist_id(youtube,channel_id)
            vi=get_video_ids(youtube,channel_id)
            v=get_video_details(youtube,vi)
            cm=comment_details(youtube,vi)
            data={'channel_details':c,
                    'playlist_details':p,
                    'video_details':v,
                    'all_comments':cm}
            return data
        
        d=main(channel_id)
        my_col.insert_one(d)
        st.write(d)


# In[16]:


# Function to list channels from MongoDB
def list_channel():     
    
    list_channels = [i['channel_details'][0]['Channel_name'] for i in my_col.find({}, {'_id': 0})]     
    return list_channels


# In[17]:


# Function to push data from MongoDB to MySQL
def sqlquery(coll, con, channel_name):
    try:
        with con.cursor() as mycursor:
            mycursor.execute("CREATE DATABASE IF NOT EXISTS YOUTUBEHARVEST1")
            mycursor.execute("USE YOUTUBEHARVEST1")
            mycursor.execute("SELECT Channel_name FROM channel")
            v= mycursor.fetchall()
            a=[]
            for i in list(v):
                a.append(i[0])
            if channel_name not in a:
                for i in coll[0]['channel_details']:
                    if i ["Channel_name"] == channel_name:
                        
                            sql = "INSERT INTO CHANNEL(Channel_name, Channel_id, Subscribers_Count, Views, Total_videos, Channel_Description, playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                            val = tuple(i.values())
                            mycursor.execute(sql,val)
                            con.commit()

                            sql1 = """INSERT INTO PLAYLIST(playlist_id,channel_id,channel_name,description,playlist_title,playlist_count,playlist_publisheddate)VALUES(%s,%s,%s,%s,%s,%s,%s)"""
                            for j in coll[0]['playlist_details']:
                                val1= tuple(j.values())
                                mycursor.execute(sql1,val1)
                            con.commit()

                            sql2="INSERT INTO VIDEOS(channel_name ,channel_id,Title,videoid ,Thumbnail,Favourite_count,description,duration,Published_date,Views,Likes,Comments_count,Definition,Caption_status)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            for j in coll[0]['video_details']:
                                val2= tuple(j.values())
                                mycursor.execute(sql2,val2)

                            con.commit()

                            sql3="INSERT INTO COMMENT(comment_Id,comment_Author,comment_Text,comment_PublishedAt,Video_Id,comment_ch_id,comment_like_count,comment_Reply_count)VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
                            for j in coll[0]['all_comments']:
                                val3= tuple(j.values())
                                mycursor.execute(sql3,val3)  

                            con.commit()
            else:
                st.write("duplicate entry")
    except pymysql.Error as e:
        st.error(f"Error in MySQL operation: {e}")
    



if selected == "MIGRATION OF DATA":
    channel_name = st.selectbox('Select the Channel', list_channel())
    coll=[]
    for i in my_col.find():
        if i['channel_details'][0]["Channel_name"] == channel_name:
            coll.append(i)
    clicked = st.button("push into SQL") 
    if clicked:
        try:
            con = pymysql.connect(host='localhost', user='root', password="RAGHU@kala1", database='YOUTUBEHARVEST1')
            sqlquery(coll, con, channel_name) 
              
            st.success("Data pushed into SQL successfully.")
        except Exception as e:
            st.error(f"Error: {e}")
                   


# In[18]:


if selected == "DATA ANALYSIS":
    st.title(f"DATA ANALYSIS ZONE")
    Question = st.selectbox(
    'select a Question',
    ('1. What are the names of all the videos and their corresponding channels?', 
     '2. which channels have the most number of videos, and how many videos do they have', 
     '3.what are the top 10 most viewed videos and their respective channels?',
     '4. How many comments were made on each video, and what are their corresponding video names?',
     '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6. what is the total number of likes for each video,and what are their corresponding video names?',
     '7. what is the total number of views for each channel, and what are thier corresponding channel names?',
     '8. What are the names of all the channels that have published videos in the year 2022?',
     '9. What is the average duration of all videos in each channel,and what are their corresponding channel names?',
     '10. which videos have the highest number of comments,and what are their corresponding channel names?'))

    if Question == '1. What are the names of all the videos and their corresponding channels?':
        query5 = "select Title as Video_name,channel_name from videos;" 
        mycursor.execute(query5)
        data1 = [i for i in mycursor.fetchall()]

        if data1:
            st.write("Result:")
            df = pd.DataFrame(data1, columns=["channel Name","Video Title"])
            st.dataframe(df)
            
        else:
            st.write("No results found")

    if Question == '2. which channels have the most number of videos, and how many videos do they have':
        query6 = "select channel_name,Total_videos as Channel_video_count from channel order by Total_videos desc;" 
        mycursor.execute(query6)
        data2 = [i for i in mycursor.fetchall()]

        if data2:
            st.write("Result:")
            df = pd.DataFrame(data2, columns=["channel Name","channel_video_count"])
            st.dataframe(df)
            st.bar_chart(df,x="channel Name",y="channel_video_count")
        else:
            st.write("No results found")

    if Question == '3.what are the top 10 most viewed videos and their respective channels?':
        query7 = "select channel_name,Title,Views as video_view_count from videos order by views desc limit 10;" 
        mycursor.execute(query7)
        data3 = [i for i in mycursor.fetchall()]

        if data3:
            st.write("Result:")
            df = pd.DataFrame(data3, columns=["channel_name","video_Name","video_view_count"])
            st.dataframe(df)
            st.bar_chart(df,x="channel_name",y="video_view_count")
        else:
            st.write("No results found")

    if Question == '4. How many comments were made on each video, and what are their corresponding video names?':
        query8 = "select Title,Comments_count  from videos order by Comments_count desc;" 
        mycursor.execute(query8)
        data4 = [i for i in mycursor.fetchall()]

        if data4:
            st.write("Result:")
            df = pd.DataFrame(data4, columns=["video_Name","channel_comment_count"])
            st.dataframe(df)
            st.line_chart(df,x="video_Name",y="channel_comment_count")
        else:
            st.write("No results found")

    if Question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        query9 = "select channel_name,Title,Likes from videos order by likes desc;" 
        mycursor.execute(query9)
        data5 = [i for i in mycursor.fetchall()]

        if data5:
            st.write("Result:")
            df = pd.DataFrame(data5, columns=["channel_name","Title","Likes"])
            st.dataframe(df)
        else:
            st.write("No results found")

    if Question == '6. what is the total number of likes for each video,and what are their corresponding video names?':
        query10 = "select channel_name,Title,Likes from videos" 
        mycursor.execute(query10)
        data6 = [i for i in mycursor.fetchall()]

        if data6:
            st.write("Result:")
            df = pd.DataFrame(data6, columns=["channel_name","Title","Likes"])
            st.dataframe(df)
        else:
            st.write("No results found")

    if Question == '7. what is the total number of views for each channel, and what are thier corresponding channel names?':
        query11 = "select channel_name,sum(views) as total_no_views from videos group by channel_name;" 
        mycursor.execute(query11)
        data7 = [i for i in mycursor.fetchall()]

        if data7:
            st.write("Result:")
            df = pd.DataFrame(data7, columns=["channel Name","Channel_View_Count"])
            st.dataframe(df)
        else:
            st.write("No results found")

    if Question == '8. What are the names of all the channels that have published videos in the year 2022?':
        query12 = "select channel_name, count(year(Published_date)) as video_count from videos where year(Published_date)=2022 group by channel_name;" 
        mycursor.execute(query12)
        data8 = [i for i in mycursor.fetchall()]

        if data8:
            st.write("Result:")
            df = pd.DataFrame(data8, columns=["channel Name","channel_video_count"])
            st.dataframe(df)
        else:
            st.write("No results found")

    if Question == '9. What is the average duration of all videos in each channel,and what are their corresponding channel names?':
        query13 = "select channel_name,round(avg(duration),3) as average from videos group by channel_name;" 
        mycursor.execute(query13)
        data9 = [i for i in mycursor.fetchall()]

        if data9:
            st.write("Result:")
            df = pd.DataFrame(data9, columns=["channel Name","channel_video_count"])
            st.dataframe(df)
        else:
            st.write("No results found")

    if Question == '10. which videos have the highest number of comments,and what are their corresponding channel names?':
        query14 = "select channel_name,Title,max(Comments_count) from videos group by channel_name,Title order by 3 desc;" 
        mycursor.execute(query14)
        data10 = [i for i in mycursor.fetchall()]

        if data10:
            st.write("Result:")
            df = pd.DataFrame(data10, columns=["channel Name","video_name","channel_comment_count"])
            st.dataframe(df)
        else:
            st.write("No results found")


# In[ ]:




