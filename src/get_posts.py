import praw
from psaw import PushshiftAPI
import datetime as dt

def get_posts(subreddit):
    """
    Function to pull all recent (24 hours) posts from a specified subreddit.

    subreddit -> Subreddit of interest.
    return -> List of posts for the day.
    """

    # set up the praw/psaw details

    r = praw.Reddit(client_id = client_id,
                    client_secret = client_secret,
                    user_agent = user_agent)

    api = PushshiftAPI(r)
    
    # set the start_epoch

    today = dt.datetime.today()
    start_epoch = int(dt.datetime(int(today.year),
                            int(today.month),
                            int(today.day)).timestamp())

    # pull all posts for today

    posts = list(api.search_submissions(after = start_epoch,
                                        subreddit = subreddit))
    
    return posts
