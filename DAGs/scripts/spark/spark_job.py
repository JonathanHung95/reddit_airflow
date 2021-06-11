from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lower, col, explode, split
from pyspark.sql.types import StringType

import argparse

# our goal is to remove columns of no interst, remove ([deleted] and [removed]) comments, change everything to lower case, strip punctuation, get part of speech -> remove everything
# thats not called a noun

# nltk function to operate on the comments
# we want to extract just nouns as that would give us the topics that the subreddit is talking about

def extract_nouns(comment):
    """
    Function to remove stop words and extract nouns from the given text.

    comment -> String from the dataframe.
    return -> Comma seperated string of nouns.
    """

    import nltk
    nltk.download("averaged_perceptron_tagger", quiet = True)
    import string

    stop_words = set(nltk.corpus.stopwords.words("english"))
    string.punctuation = string.punctuation + "’"

    new_comment = comment.translate(str.maketrans("", "", string.punctuation))
    new_comment = nltk.word_tokenize(new_comment)
    filtered_comment = [w for w in new_comment if not w.lower() in stop_words]
    word_list = nltk.pos_tag(filtered_comment)

    nouns = []

    for word, pos in word_list:
        if (pos == "NN" or pos == "NNP" or pos == "NNS" or pos == "NNPS"):
            nouns.append(word)

    return ",".join(nouns)

spark_extract_nouns = udf(extract_nouns, StringType())

# function containing our transformations

def transform_data(input_loc, output_loc):
    comments_df = spark.read.option("header", True).csv(input_loc)

    comments_df = comments_df.drop("time", "comment_id", "link_id")\
                        .where("comment != '[deleted]' or comment != '[removed]'")\
                        .withColumn("lower_comment", lower(col("comment")))

    new_df = comments_df.withColumn("nouns", spark_extract_nouns(comments_df.lower_comment))

    # group the nouns by frequency and that's our actual final data from the spark job

    count_df = new_df.withColumn("word", explode(split(col("nouns"), ",")))\
                        .groupBy("word")\
                        .count()\
                        .sort("count", ascending = False)\
                        .limit(100)

    count_df.write.mode("overwrite").parquet(output_loc)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type = str, help = "HDFS input", default = "/input")
    parser.add_argument("--ouput", type = str, help = "HDFS output", default = "/output")
    args = parser.parse_args()

    spark = SparkSession.builder.\
                    .appName("wcd_spark_job")\
                    .getOrCreate()

    transform_data(input_loc = args.input, output_loc = args.output)

