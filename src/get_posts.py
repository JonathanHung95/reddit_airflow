import praw
import datetime as dt

def get_posts(subreddit, hour_ago):
    """
    Function to pull all recent (every hour) posts from a specified subreddit.

    subreddit -> Subreddit of interest.
    hour_ago -> Unix time for an hour ago.
    return -> List of lists [time, id, title, score].
    """

    # set up the praw details

    r = praw.Reddit(client_id = client_id,
                    client_secret = client_secret,
                    user_agent = user_agent)

    subreddit = r.subreddit(subreddit)

    # pull all posts for the last hour
    # create a list of lists [time, id, title, score]

    posts = []

    for submission in subreddit.new(limit = 100):
        if submission.created_utc > hour_ago:
            posts.append([submission.created_utc, submission.id, submission.title, submission.score])

        else:
            # since the postings are in descending order of created time
            # we can stop looping when we've hit the posts that are older than an hour_ago
            break
    
    return posts
