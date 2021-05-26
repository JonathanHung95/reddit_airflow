import praw
import datetime as dt

def get_comments(subreddit, hour_ago):
    """
    Function to retrieve recent (last hour) comments from a given subreddit.

    subreddit -> Subreddit of interest.
    hour_ago -> Unix time for an hour ago.
    return -> List of lists [time, comment_id, link_id, comment, score]
    """

    # set up praw details

    r = praw.Reddit(client_id = client_id,
                    client_secret = client_secret,
                    user_agent = user_agent)

    subreddit = r.subreddit(subreddit)

    # grab our most recent comments and filter by hour_ago
    # we only want to grab the newest comments
    # create a list of lists from the metadata [time, comment_id, link_id, comment, score]

    comments = []

    for comment in subreddit.comments(limit = 100):
        if comment.created_utc > hour_ago:
            # manipulate the link_id a bit, we don't want the level (ie. t1_123456 -> 123456)

            link_id = comment.link_id.split("_")[1]
            comments.append([comment.created_utc, comment.id, link_id, comment.body, comment.score])
        
        else:
            # stop appending if we hit comments from > hour ago
            break

    return comments