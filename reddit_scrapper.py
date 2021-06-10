import praw
import datetime as dt
import csv

import yaml

# idea here is to scrap the comments of a given subreddit -> store in a csv file 

# set up functions to carry out the scrapping

def get_comments(subreddit, hour_ago, client_id, client_secret, user_agent):
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

def write_csv(list_to_write, header, file_name):
    """
    Write a csv file from list_to_write.  This should be a list of lists.  We're writing a function like this because the
    posts and comments scrapped need to be sanitized a bit (remove "&#x200B", "\n" etc.)

    list_to_write -> List of lists of posts/comments.
    header -> Header for the csv file; should be a list.
    file_name -> Name of the file.
    return -> None.
    """

    # iterate over the list of lists
    # apply a map + lambda function to clean

    for i in range(0, len(list_to_write)):
        list_to_write[i] = list(map(lambda x: str(x).replace("\n", "").replace("&#x200B", ""), list_to_write[i]))

    # write to csv file

    with open(file_name, "w", newline = "", encoding = "utf-8") as file:
        writer = csv.writer(file)

        # write our headers first

        writer.writerow(header)

        # iterate to write our rows from list_to_write

        for i in range(0, len(list_to_write)):
            writer.writerow(list_to_write[i])

    return None

# actual main part of the script now

if __name__ == "__main__":
    with open("config.yml", "r") as f: 
        data = yaml.safe_load(f)

    client_id = data["client_id"]
    client_secret = data["client_secret"]
    user_agent = data["user_agent"]
    subreddit = data["subreddit"]

    # get the unix time for an hour ago
    # get the time here so that we can keep things consistent between get_posts and get_comments

    hour_ago = (dt.datetime.now() - dt.timedelta(hours = 1)).timestamp()

    # scrap the last hour's worth of data

    posts = get_posts(subreddit, hour_ago, client_id, client_secret, user_agent)
    write_csv(posts, ["time", "id", "title", "score"], "posts.csv")

    comments = get_comments(subreddit, hour_ago, client_id, client_secret, user_agent)
    write_csv(comments, ["time", "comment_id", "link_id", "comment", "score"], "comments.csv")
