import praw
import datetime as dt
import csv
import boto3

def lambda_handler(event, context):
    print("Jobs started!")
    
    ACCESS_KEY_ID = ACCESS_KEY_ID
    ACCESS_SECRET_KEY = ACCESS_SECRET_KEY
    BUCKET_NAME = "jonathan-wcd-midterm"
    
    s3 = boto3.resource("s3",
                        aws_access_key_id=ACCESS_KEY_ID,
                        aws_secret_access_key=ACCESS_SECRET_KEY)
    bucket = s3.Bucket(BUCKET_NAME)
    key = "comments.csv"
    
    client_id = CLIENT_ID
    client_secret = CLIENT_SECRET
    user_agent = USER_NAME
    subreddit = "worldnews"
    hour_ago = (dt.datetime.now() - dt.timedelta(hours = 1)).timestamp()

    r = praw.Reddit(client_id = client_id,
                    client_secret = client_secret,
                    user_agent = user_agent)
    
    subreddit = r.subreddit(subreddit)

    comments = []

    for comment in subreddit.comments(limit = 1000):
        if comment.created_utc > hour_ago:
            comments.append([comment.created_utc, comment.id, comment.link_id, comment.body, comment.score])
        
        else:
            # stop appending if we hit comments from > hour ago
            break

    for i in range(0, len(comments)):
        comments[i] = list(map(lambda x: str(x).replace("\n", "").replace("&#x200B", ""), comments[i]))

    with open("/tmp/" + key, "w", newline = "", encoding = "utf-8") as file:
        writer = csv.writer(file)

        writer.writerow(["time", "comment_id", "link_id", "comment", "score"])

        for i in range(0, len(comments)):
            writer.writerow(comments[i])
    
    bucket.upload_file("/tmp/" + key, "landing/" + key)
    
    print("Jobs done!")