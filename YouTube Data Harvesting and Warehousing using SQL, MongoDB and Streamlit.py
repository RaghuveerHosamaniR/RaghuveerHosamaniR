#!/usr/bin/env python
# coding: utf-8

# In[1]:


#importing the necessary libraries
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from PIL import Image


# In[78]:


# Importing libraries

import googleapiclient.discovery
from googleapiclient.discovery import build

import json
import re

import pymongo

import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql

import pandas as pd
import numpy as np

import streamlit as st
import plotly.express as px
from googleapiclient.discovery import build


# In[2]:


api_key="AIzaSyBlFgPHuT23d7p5FocmnN8hW5U9PT5K57I"
channel_ids=['UCr-gTfI7au9UaEjNCbnp_Nw',# vidy vox
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


# In[3]:


#to get channel data
def get_channel_stats(youtube, channel_ids):
    yc_data=[]
    request=youtube.channels().list(part="snippet,contentDetails,statistics",
                                    id=','.join(channel_ids))
    channel_data=request.execute()
    for i in range(len(channel_data['items'])):
        data =dict(channel_name=channel_data['items'][i]['snippet']['title'],
                  channel_id=channel_data['items'][i]['id'],
                   video_counts=channel_data['items'][i]['statistics']['videoCount'],
                  subscription_count=channel_data['items'][i]['statistics']['subscriberCount'],
                  Channel_views=channel_data['items'][i]['statistics']['viewCount'],
                  Channel_Description=channel_data['items'][i]['snippet']['description'],
                  playlist_id= channel_data['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                  )
        yc_data.append(data)
    return yc_data


# In[4]:


get_channel_stats(youtube, channel_ids)


# In[ ]:


import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
mycol = mydb["customers"]

mydict = { "name": "John", "address": "Highway 37" }

x = mycol.insert_one(mydict)


# In[5]:


channel_stastitics = get_channel_stats(youtube, channel_ids)
channel_stat=pd.DataFrame(channel_stastitics)
channel_stat


# In[6]:


channel_stat.dtypes


# In[7]:


import numpy as np

channel_stat['video_counts']=pd.to_numeric(channel_stat['video_counts'])
channel_stat['subscription_count']=pd.to_numeric(channel_stat['subscription_count'])
channel_stat['Channel_views']=pd.to_numeric(channel_stat['Channel_views'])
channel_stat.dtypes


# In[8]:


#videos_data
#request = youtube.playlistItems().list(
# part="snippet,contentDetails",
#playlistId=playlists)
#video_data = request.execute()
#print(video_data)


# In[9]:


import seaborn as sns
sns.set(rc={'figure.figsize':(22,7)})
ax = sns.barplot(x = 'channel_name', y = 'subscription_count', data=channel_stat)


# In[10]:


ax = sns.barplot(x = 'channel_name', y = 'Channel_views', data=channel_stat)


# In[11]:


ax = sns.barplot(x = 'channel_name', y = 'video_counts', data=channel_stat)


# # function to get video ids

# In[48]:


playlist_id_=channel_stat.loc[channel_stat['channel_name']=="Arijit Singh",'playlist_id'].iloc[0]


# In[47]:


playlist_id


# In[49]:


#function to get video ids
def get_video_ids(youtube,playlist_id):
    request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=playlist_id,
        maxResults=50)
    response=request.execute()
    
    
    video_ids=[]
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
        next_page_token=response.get('nextPageToken')
        more_pages=True
        
        
    while more_pages:
        if  next_page_token is None:
            more_pages=False
                
        else:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token)
                    
            response=request.execute()
                    
                    
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
                
                next_page_token=response.get('nextPageToken')
                        
    return (video_ids)


# In[51]:


video_ids=get_video_ids(youtube,playlist_id)


# In[16]:


video_ids


# In[17]:


len(video_ids)


# In[18]:


#get video details
def get_video_details(youtube,video_ids):
    all_video_stat=[]
    
    for i in range(0,len(video_ids),50):
        request=youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids[i:i+50]))
    
        response=request.execute()
    
        for video in response['items']:
            video_stat=dict(Title=video['snippet']['title'],
                           Published_date=video['snippet']['publishedAt'],
                           Views=video['statistics']['viewCount'],
                            Likes=video['statistics']['viewCount'],
                           Comments=video['statistics']['commentCount'])
            all_video_stat.append(video_stat)

        
    return (all_video_stat)


# In[33]:


video_details=get_video_details(youtube,video_ids)


# In[35]:


video_details


# In[36]:


import numpy as np
import pandas as pd
video_data=pd.DataFrame(video_details)
video_data


# In[37]:


video_data.dtypes


# In[39]:


import numpy as np
import pandas as pd
video_data['Published_date']=pd.to_datetime(video_data["Published_date"]).dt.date
video_data['Views']=pd.to_numeric(video_data['Views'])
video_data['Likes']=pd.to_numeric(video_data['Likes'])
video_data['Comments']=pd.to_numeric(video_data['Comments'])
video_data


# In[40]:


video_data.dtypes

