import streamlit as st
from streamlit_option_menu import option_menu
from PIL import Image
from io import BytesIO
import time
import requests
from config import*
from fetch_data import *
from Migrate_to_SQL import*
from Questions import*

#page congiguration
st.set_page_config(page_title= "Youtube Analysis",
                   page_icon= '📈',
                   layout= "wide",)


navigation,data=st.columns([1,4.55])
with navigation:


    selected = option_menu('Main Menu', ['HOME',"CHANNEL FETCH","DATABASE MIGRATION","CHANNEL INSIGHTS"],
                       icons=["house-fill",'youtube','box-arrow-in-up-right','bar-chart-fill'],default_index=0)#cloud-check##
with data:
    st.markdown("<h1 style='text-align: left; color: black;'>DATA HARVESTING AND WARHOUSING</h1>", unsafe_allow_html=True)

with data:
    if selected=='HOME':
        st.markdown('###  ***:black[WELCOME TO YOUTUBE CHANNEL DATA ANALYSIS]***')

    #this option for channel information
    if selected=='CHANNEL FETCH':
        left,center,right=st.columns([3.5,5.5,2])
        with left:
            channel_id=st.text_input('Enter channel ID:')
            if st.button('SEARCH'):
                with st.spinner('Fetching channel information...'):
                    time.sleep(2)
                with center:
                    channel_data,channel_image = get_channel_data(y_tube, channel_id)
                    df = pd.DataFrame(channel_data, index=['Channel Information'])
                    channel = df.T
                    name=channel_data["channel_name"]
                    st.write(f"##### ***:red[You searched] {name} :red[youtube channel]***")
                    st.dataframe(channel,width=400)
                with right:
                    #channel thumbnails
                    if channel_data:
                        image = channel_image['image']
                        response = requests.get(image)
                        if response.status_code == 200:
                            image = Image.open(BytesIO(response.content))
                            st.image(image, caption=f' {name} ',width=100)
                        else:
                            print('Failed to load an image.')
                    else:
                        print('Failed to get channel image')


#video_info and comment_info

            if st.button('UPLOAD TO MONGODB DATABASE'):
                with st.spinner('uploading channel information ...'):
                    with left:
                        # ===========================FETCHING CHANNEL,VIDEO,COMMENT FROM MONGODB DATABASE========================================
                        def channel_video_comment():
                            channel_data, channel_image = get_channel_data(y_tube, channel_id)
                            playlist_id = channel_data['playlist_id']
                            video_id = get_video_ids(y_tube, playlist_id)
                            video_data = get_video_data(y_tube, video_id)
                            comment_data = get_comment_data(y_tube, video_id)
                            channel = {
                                'channel_info': channel_data,
                                'video_info': {}
                            }
                            for count, vid_data in enumerate(video_data, 1):
                                v_id = f"Video_Id_{count}"
                                cmt = {}
                                for i in comment_data:
                                    if i["Video_id"] == vid_data["video_id"]:
                                        c_id = f"Comment_Id_{len(cmt) + 1}"
                                        cmt[c_id] = {
                                            "Comment_Id": i.get("Comment_Id", 'comments_disabled'),
                                            "Comment_Text": i.get("Comment_Text", 'comments_disabled'),
                                            "Comment_Author": i.get("Comment_Author", 'comments_disabled'),
                                            "Comment_Published_At": i.get("Comment_Published_At", 'comments_disabled')
                                        }
                                vid_data["Comments"] = cmt
                                channel['video_info'][v_id] = vid_data
                            return channel
                        channel=channel_video_comment()
                        try:
                            check_existing_document = coll.find_one({"channel_info.channel_id": channel_id})
                            if check_existing_document is None:
                                coll.insert_one(channel)
                                st.success('Successfully uploaded ',icon='✔️')
                                st.info('Please select an option Database to view and migrate the channel data',icon='ℹ️')
                            else:
                                st.error("  OOPS  channel_ID already uploaded Try with different channel_ID",icon='❕')
                        except Exception as e:
                            print(f"Error occurred while uploading channel information: {str(e)}")


    if selected == 'DATABASE MIGRATION':
        nosql,sql=st.columns([2,2])
# fetching data from mongoDB database==================================================================================
        with nosql:
            st.markdown("## MONGODB DATABASE - ***NOSQL***")
            with st.spinner('Fetching ...'):
                channel_list=channel_list()
                option=st.selectbox('UPLOADED CHANNEL NAME LIST',['Please select channel name']+channel_list)

        for result in coll.find({"channel_info.channel_name": option}):
            channel_info = result['channel_info']
            video_info = result['video_info']
            st.write("### JSON DISPLAY ")
            st.write(channel_info,video_info)


# migrating data from sqlite
#  database====================================================================================
        with sql:
            st.markdown("## :green[SQLite DATABASE - ***SQL***]")
            st.markdown(':green[MIGRATE DATA FROM NOSQL TO SQL] ')
            if st.button('MIGRATE TO SQL'):
                with st.spinner('Migrating ...'):
                    channel_name_to_find = option
                    channel_df, playlist_df,video_df, comment_df = NOSQL_TO_SQL(channel_name_to_find)
                    # Migrate data to SQL database
                    channel_df.to_sql('channel_data', con=eng, if_exists='append', index=False,schema=None)
                    playlist_df.to_sql('playlist_data', con=eng, if_exists='append', index=False,schema=None)
                    video_df.to_sql('video_data', con=eng, if_exists='append', index=False,schema=None)
                    comment_df.to_sql('comment_data', con=eng, if_exists='append', index=False,schema=None)
                    st.success(f"{channel_name_to_find} channel migrated successfully", icon='✅')



    if selected == 'CHANNEL INSIGHTS':
        st.markdown('## CHANNEL ANALYSIS')
        select_question = st.selectbox("RAISE QUERY FROM DATABASE", ('Select question here',
                                                                            '1. What are the names of all the videos and their corresponding channels?',
                                                                            '2. Which channels have the most number of videos, and how many videos do they have?',
                                                                            '3. What are the top 10 most viewed videos and their respective channels?',
                                                                            '4. How many comments were made on each video, and what are their corresponding video names?',
                                                                            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                                                            '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                                                            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                                                            '8. What are the names of all the channels that have published videos in the year 2022?',
                                                                            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                                                            '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

        if select_question== '1. What are the names of all the videos and their corresponding channels?':

            st.write(question_1())
        elif select_question=='2. Which channels have the most number of videos, and how many videos do they have?':
            st.write(question_2())
        elif select_question=='3. What are the top 10 most viewed videos and their respective channels?':
            st.write(question_3())
        elif select_question=='4. How many comments were made on each video, and what are their corresponding video names?':
            st.write(question_4())
        elif select_question=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            st.write(question_5())
        elif select_question=='6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            st.write(question_6())
        elif select_question=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
            st.write(question_7())
        elif select_question=='8. What are the names of all the channels that have published videos in the year 2022?':
            st.write(question_8())
        elif select_question=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            st.write(question_9())
        elif select_question== '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            st.write(question_10())

