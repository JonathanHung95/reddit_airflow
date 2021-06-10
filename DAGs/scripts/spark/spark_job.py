from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lower, col, explode, split
from pyspark.sql.types import StringType

# our goal is to remove columns of no interst, remove ([deleted] and [removed]) comments, change everything to lower case, strip punctuation, get part of speech -> remove everything
# thats not called a noun

spark = SparkSession.builder.master("yarn").\
                    .appName("wcd_spark_job")\
                    .getOrCreate()

# spark configs

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIASVOJD6ASMLJIPD2B")

access_id = "AKIASVOJD6ASJKM6GCVL"
access_key = "QSr40gjCIBffejzbp0kJCpAQHbwq5LHj+b0U+q2l"

# import our csv file

comments_df = spark.read.csv("comments.csv", header = True)

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

# apply functions to clean up the dataframe + transform 

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

count_df = new_df.withColumn("word", explode(split(col("nouns"), ","))).groupBy("word").count().sort("count", ascending = False).limit(100)
