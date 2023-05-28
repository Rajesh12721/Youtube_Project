import googleapiclient.discovery
import pymongo
from pymongo import MongoClient
import mysql.connector
import streamlit as slt
#MongoDB Connection
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client['youtube']
collection = db['Channeldatas']
#MySQL Connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345",
    database = "youtube",
    auth_plugin='mysql_native_password'
)
mycursor = mydb.cursor()
selectChannel = []
slt.title("Youtube Data Fetching")
channel_id = slt.sidebar.text_input("Enter the Channel ID")
overall = {}
#API Connection
api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = "AIzaSyCL9HIy8Z6JS9Xngv5GJzQQkXK0MnkjloI"
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=DEVELOPER_KEY)
#VideoID Getting
def fetch_all_youtube_videos(playlistId):
    res = youtube.playlistItems().list(
        part="snippet",
        playlistId=playlistId,
        maxResults="50",
        fields="items(snippet(resourceId(videoId)))"
    ).execute()

    nextPageToken = res.get('nextPageToken')
    while ('nextPageToken' in res):
        nextPage = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlistId,
            maxResults="50",
            pageToken=nextPageToken,
            fields="items(snippet(resourceId(videoId)))"
        ).execute()
        res['items'] = res['items'] + nextPage['items']

        if 'nextPageToken' not in nextPage:
            res.pop('nextPageToken', None)
        else:
            nextPageToken = nextPage['nextPageToken']

    return res
#Comments Getting
def comments_func(v_id):
    videocomm = youtube.commentThreads().list(
        part='id,snippet',
        videoId=v_id
    ).execute()
    Comment = {}
    j = 1
    for item in videocomm['items']:
        commentid = item['snippet']['topLevelComment']['id']
        commenttext = item['snippet']['topLevelComment']['snippet']['textOriginal']
        commentauthor = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
        commentpub = item['snippet']['topLevelComment']['snippet']['publishedAt']
        comments = {
            "comment_"+str(j): {
                'id': commentid,
                'text': commenttext,
                'name': commentauthor,
                'publishedat': commentpub
            }
        }
        Comment.update(comments)
        j += 1
    return Comment
#Channel Details Getting
c = youtube.channels().list(
    part="id,snippet,contentDetails,statistics,status",
    id=channel_id,
    fields="items(id,snippet,contentDetails,statistics,status)"
).execute()
cname = c['items'][0]['snippet']['title']
cid = c['items'][0]['id']
csubscribe = c['items'][0]['statistics']['subscriberCount']
cviews = c['items'][0]['statistics']['viewCount']
cdiscription = c['items'][0]['snippet']['localized']['description']
cstatus = c['items'][0]['status']['privacyStatus']
playlists = youtube.playlists().list(
    part="snippet,contentDetails",
    channelId=cid,
    maxResults=50,
    fields="items(id,snippet(title))"
).execute()
channel = {
    cname: {
        'name': cname,
        'id': cid,
        'subscript_count': csubscribe,
        'channel_views': cviews,
        'channel_descript': cdiscription,
        'channel status' : cstatus,
        'playlist_id': playlists
    }
}
overall.update(channel)
slt.write(channel)
video_ids = []
#VideoID Getting
for playlist in playlists['items']:
    play_l = playlist['id']
    returndata = fetch_all_youtube_videos(play_l)
    for vdata in returndata['items']:
        vid = vdata['snippet']['resourceId']['videoId']
        video_ids.append(vid)
#Video Details Getting
i=1
for video_id in video_ids:
    v = youtube.videos().list(
        part="id,snippet,contentDetails,statistics",
        id=video_id,
        fields="items(id,snippet,contentDetails,statistics)"
    ).execute()
    try:
        idv = v['items'][0]['id']
        namev = v['items'][0]['snippet']['title']
        disv = v['items'][0]['snippet']['description']
        pubv = v['items'][0]['snippet']['publishedAt']
        viewv = v['items'][0]['statistics']['viewCount']
        favv = v['items'][0]['statistics']['favoriteCount']
        commentcv = v['items'][0]['statistics']['commentCount']
        durationv = v['items'][0]['contentDetails']['duration']
        thumbnailv = v['items'][0]['snippet']['thumbnails']
        captionv = v['items'][0]['contentDetails']['caption']
        like_v = v['items'][0]['statistics']["likeCount"]
        dislikev = v['items'][0]['statistics']['dislikeCount']
        tagsv = v['items'][0]['snippet']['tags']
    except KeyError :
        dislikev = None
        tagsv = None
    except:
        pass
    videos = {
        'Video_'+str(i) : {
        'id': idv,
        'name': namev,
        'discription': disv,
        'tags': tagsv,
        'publishedat': pubv,
        'views': viewv,
        'likes': like_v,
        'dislikes': dislikev,
        'fav_count': favv,
        'comment_count': commentcv,
        'duration': durationv,
        'thumbnail': thumbnailv,
        'caption_status': captionv,
        'comments': comments_func(idv) 
        }
    }
    i += 1
    overall.update(videos)
#Migrate To MongoDB
migrate = slt.sidebar.button('Migrate to MongoDB')
if migrate:
    collection.insert_one(overall)
for sc in selectChannel:
    if sc == channel_id:
        break
else:
    selectChannel.append(channel_id)
select = slt.sidebar.selectbox("Select the Channel Id:",selectChannel)
slt.write("User Select the channel id is",select)
#MongoDB to MySQL  data Convertion
to_sql = slt.sidebar.button('MongoDB to MySQL')
if to_sql:
    mycursor.execute("create table channeldetails(channel_id varchar(255),channel_name varchar(255),channel_subscription_count int,channel_views int,channel_description text,channel_status varchar(255))")
    mycursor.execute("create table playlistdetails(playlist_id varchar(255),channel_id varchar(255),playlistname varchar(255))")
    mycursor.execute("create table videodetails(video_id varchar(255),video_name varchar(255),video_description text,published_date varchar(255),view_count int,like_count int,dislike_count int,favorite_count int,comment_count int,duration varchar(255),thumbnail text,caption_status varchar(255))")
    mycursor.execute("create table commentdetails(comment_id varchar(255),video_id varchar(255),comment_text text,comment_author varchar(255),comment_published_date varchar(255))")

    collect = collection.find()
    for values in collect:
        keys = []
        for value in values:
            keys.append(value)
        channel = keys[1]
        cid = values[channel]['id']
        cname = values[channel]['name']
        csub = values[channel]['subscript_count']
        cview = values[channel]['channel_views']
        cdis = values[channel]['channel_descript']
        cstatus = values[channel]['channel status']
        queryc = "insert into channeldetails (channel_id,channel_name,channel_subscription_count,channel_views,channel_description,channel_status) values (%s,%s,%s,%s,%s,%s)"
        valuec = (cid,cname,csub,cview,cdis,cstatus)
        mycursor.execute(queryc,valuec)
        cplay = values[channel]['playlist_id']
        for play in cplay:
            pls = values[channel]['playlist_id'][play]
            for pl in pls:
                plid = pl['id']
                plname = pl['snippet']['title']
                queryp = "insert into playlistdetails (playlist_id,channel_id,playlistname) values (%s,%s,%s)"
                valuep = (plid,cid,plname)
                mycursor.execute(queryp,valuep)
        for item in keys[2:]:
            y = values[item]
            vid = y['id']
            vname = y['name']
            vdis = y['discription']
            vpub = y['publishedat']
            vview = y['views']
            vlike = y['likes']
            vdislike = y['dislikes']
            vfav = y['fav_count']
            vcommc = y['comment_count']
            vdur = y['duration']
            vthumb = str(y['thumbnail'])
            vcaption = y['caption_status']
            vcomments = y['comments']
            queryv = "insert into videodetails (video_id,video_name,video_description,published_date,view_count,like_count,dislike_count,favorite_count,comment_count,duration,thumbnail,caption_status) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            valuev = (vid,vname,vdis,vpub,vview,vlike,vdislike,vfav,vcommc,vdur,vthumb,vcaption)
            mycursor.execute(queryv,valuev)
            for com in vcomments:
                commentid = y['comments'][com]['id']
                commenttext = y['comments'][com]['text']
                commentname = y['comments'][com]['name']
                commentpub = y['comments'][com]['publishedat']
                querycom = "insert into commentdetails (comment_id,video_id,comment_text,comment_author,comment_published_date) values (%s,%s,%s,%s,%s)"
                valuecom = (commentid,vid,commenttext,commentname,commentpub)
                mycursor.execute(querycom,valuecom)

        mydb.commit()
    mydb.close()
    slt.write("MongoDB to MySQL Migrate Successfully!")
