import pandas as pd
from config import*
import threading
import sqlite3


# Create a thread-local storage to hold SQLite objects
thread_local = threading.local()

def get_db_connection():
    if not hasattr(thread_local, "connection"):
        thread_local.connection = sqlite3.connect("y_tube1_data.db")
    return thread_local.connection

#========================RAISE SQL QUERY TO SOLVE QUESTION =============================================================
def question_1():
    connection = get_db_connection()
    my_cursor = connection.cursor()
    my_cursor.execute("SELECT channel_data.channel_name, video_data.video_name FROM channel_data JOIN video_data ON channel_data.channel_name = video_data.channel_name;")
    data = my_cursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_NAME']).reset_index(drop=True)
    data_df.index += 1
    return data_df

def question_2():
    connection = get_db_connection()
    my_cursor = connection.cursor()
    my_cursor.execute("SELECT channel_data.channel_name, COUNT(video_data.video_id) AS video_count FROM channel_data JOIN video_data ON channel_data.channel_name = video_data.channel_name GROUP BY channel_data.channel_name ORDER BY video_count DESC;")
    data = my_cursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df

def question_3():
    connection = get_db_connection()
    my_cursor = connection.cursor()
    print("TOP 10 CHANNEL VIEWS")
    my_cursor.execute(
        "SELECT channel_data.channel_name,video_data.video_name,  video_data.view_count FROM video_data JOIN channel_data ON video_data.channel_name = channel_data.channel_name ORDER BY video_data.view_count DESC LIMIT 10;")
    data = my_cursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_NAME', 'VIEW_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df

def question_4():
    connection = get_db_connection()
    my_cursor = connection.cursor()
    my_cursor.execute("SELECT video_name, comment_count from video_data ORDER BY comment_count DESC;")
    data = my_cursor.fetchall()
    data_df = pd.DataFrame(data, columns=['VIDEO_NAME', 'COMMENT_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df
def question_5():
    connection = get_db_connection()
    my_cursor = connection.cursor()
    my_cursor.execute("SELECT  channel_data.channel_name, video_data.video_name,video_data.like_count FROM video_data JOIN channel_data ON video_data.channel_name = channel_data.channel_name ORDER BY video_data.like_count DESC;")
    data = my_cursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_NAME','LIKE_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df
def question_6():
    connection = get_db_connection()
    my_cursor = connection.cursor()
    my_cursor.execute("SELECT video_name, like_count, dislike_count FROM video_data ORDER BY like_count DESC, dislike_count ASC;")
    data = my_cursor.fetchall()
    data_df = pd.DataFrame(data, columns=['VIDEO_NAME', 'LIKE_COUNT','DISLIKE_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df
def question_7():
    connection = get_db_connection()
    my_cursor = connection.cursor()
    my_cursor.execute("SELECT channel_name, channel_views FROM channel_data ORDER BY channel_views DESC;")
    data= my_cursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'TOTAL_CHANNEL_VIEWS']).reset_index(drop=True)
    data_df.index += 1
    return data_df
def question_8():
    connection = get_db_connection()
    my_cursor = connection.cursor()
    my_cursor.execute("SELECT channel_data.channel_name, video_data.video_name, video_data.published_date FROM channel_data JOIN video_data ON channel_data.channel_name = video_data.channel_name WHERE strftime ('%Y', video_data.published_date) = '2022';")
    data= my_cursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_NAME', '2022_VIDEO']).reset_index(drop=True)
    data_df.index += 1
    return data_df
def question_9():
    connection = get_db_connection()
    my_cursor = connection.cursor()
    my_cursor.execute("SELECT channel_data.channel_name, ROUND(AVG(video_data.duration), 2) AS average_duration FROM channel_data JOIN video_data ON channel_data.channel_name = video_data.channel_name GROUP BY channel_data.channel_name ;")
    data = my_cursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'AVERAGE_DURATION']).reset_index(drop=True)
    data_df.index += 1
    return data_df

def question_10():
    connection = get_db_connection()
    my_cursor = connection.cursor()
    my_cursor.execute("SELECT channel_name, video_name, comment_count FROM video_data ORDER BY comment_count DESC;")
    data = my_cursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_NAME','COMMENT_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df