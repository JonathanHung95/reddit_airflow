import praw
import datetime as dt

from src.get_posts import get_posts
from src.get_comments import get_comments

# get the unix time for an hour ago
# get the time here so that we can keep things consistent between get_posts and get_comments

hour_ago = (dt.datetime.now() - dt.timedelta(hours = 1)).timestamp()