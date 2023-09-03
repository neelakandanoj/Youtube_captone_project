from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import create_engine
import pymongo as pm
import sqlite3 as sq

#google API key
api_k='AIzaSyB9ChD-k9fy-bMGNKqMoGjQd4HeX_noYc0'
y_tube = build('youtube', 'v3', developerKey=api_k)

#Mongo connection
client_D=pm.MongoClient('mongodb+srv://neelakandan:Neelakandan2001@neelakandan.f2q560b.mongodb.net/?retryWrites=true&w=majority')
db=client_D['youtube_details']
coll = db["channel_info"]

#sqlalchemy engine
eng=create_engine('sqlite:///y_tube1_data.db')

#sqlite3
mydb=sq.connect(database='y_tube1_data.db')
my_cursor=mydb.cursor()