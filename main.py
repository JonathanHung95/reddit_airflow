import praw
import datetime as dt
import csv

from src.get_posts import get_posts
from src.get_comments import get_comments
from src.write_csv import write_csv

import yaml

# idea here is to scrap the posts and comments from a given subreddit and save them into seperate csv files
# we can recombine them later, or maybe here; still not too sure about this part
# at some point, we'll want to upload those csvs to an S3 bucket -> job for airflow I suspect
# this gives us a dataset to work with for the project -> am I going overboard with this?

with open("config.yml", "r") as f:
    data = yaml.safe_load(f)

client_id = data["client_id"]
client_secret = data["client_secret"]
user_agent = data["user_agent"]

# get the unix time for an hour ago
# get the time here so that we can keep things consistent between get_posts and get_comments

hour_ago = (dt.datetime.now() - dt.timedelta(hours = 1)).timestamp()

# scrap the last hour's worth of data
# for now, we use a small subreddit "EpicSeven"

posts = get_posts("EpicSeven", hour_ago, client_id, client_secret, user_agent)
write_csv(posts, ["time", "id", "title", "score"], "posts.csv")

comments = get_comments("EpicSeven", hour_ago, client_id, client_secret, user_agent)
write_csv(comments, ["time", "comment_id", "link_id", "comment", "score"], "comments.csv")
