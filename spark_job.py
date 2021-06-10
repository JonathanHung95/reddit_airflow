import pyspark.sql.types
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lower, col, explode, split
from pyspark.sql.types import StringType

# our goal is to remove columns of no interst, remove ([deleted] and [removed]) comments, change everything to lower case, strip punctuation, get part of speech -> remove everything
# thats not called a noun

spark = SparkSession.builder.appName("wcd_spark_job").getOrCreate()

# import our csv file

comments_df = spark.read.csv("comments.csv", header = True)

# nltk function to operate on the comments
# we want to extract just nouns as that would give us the topics that the subreddit is talking about

def extract_nouns(comment):
    # simple function that uses nltk to pull the nouns out of the text
    # returns a string of nouns

    import nltk
    nltk.download("averaged_perceptron_tagger", quiet = True)
    import string

    new_comment = comment.translate(str.maketrans('', '', string.punctuation))
    new_comment = nltk.word_tokenize(new_comment)
    word_list = nltk.pos_tag(new_comment)

    nouns = []

    for word, pos in word_list:
        if (pos == "NN" or pos == "NNP" or pos == "NNS" or pos == "NNPS"):
            nouns.append(word)

    return ",".join(nouns)

spark_extract_nouns = udf(extract_nouns, StringType())

# apply function to clean up the dataframe + transform 

new_df = comments_df.drop("time", "comment_id", "link_id")\
                    .where("comment != '[deleted]' or comment != '[removed]'")\
                    .withColumn("comment", lower(col("comment")))\
                    .withColumn("nouns", spark_extract_nouns(comments_df.comment))

# group the nouns by frequency and that's our actual final data for the spark job

count_df = new_df.withColumn("word", explode(split(col("nouns"), ",")))\
                    .groupBy("word")\
                    .count()\
                    .sort("count", ascending = False)





