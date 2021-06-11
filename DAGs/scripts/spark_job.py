from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lower, col, explode, split
from pyspark.sql.types import StringType
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains

import argparse


def transform_data(input_loc, output_loc):
    """
    Simple function to clean up data and transform it.
    """

    # some helper functions

    def strip_punctuation(x):
        punctuation = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
        for i in punctuation:
            x = x.replace(i, "")
        return x

    def flatten_column(x):
        return ",".join(x)

    spark_strip_punctuation = udf(strip_punctuation, StringType())
    spark_flatten_column = udf(flatten_column, StringType())

    # cleaning

    comments_df = spark.read.option("header", True).csv(input_loc)

    comments_df = comments_df.drop("time", "comment_id", "link_id", "score")\
                        .where("comment != '[deleted]' or comment != '[removed]'")\
                        .withColumn("lower_comment", lower(col("comment")))

    comments_df = comments_df.withColumn("cleaned_comment", spark_strip_punctuation("lower_comment"))

    tokenizer = Tokenizer(inputCol = "cleaned_comment", outputCol = "comment_tokens")
    token_df = tokenizer.transform(comments_df).select("comment_tokens")

    remover = StopWordsRemover(inputCol = "comment_tokens", outputCol = "comment_clean")
    new_df = remover.transform(token_df).select("comment_clean")

    new_df = new_df.withColumn("flattened_comment", spark_flatten_column("comment_clean"))

    count_df = new_df.withColumn("word", explode(split(col("flattened_comment"), ",")))\
                        .groupBy("word")\
                        .count()\
                        .sort("count", ascending = False)\
                        .limit(100)
    
    count_df = count_df.where("word != ''")

    # create our new data

    count_df.write.mode("overwrite").csv(output_loc + "cleaned_data.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type = str, help = "HDFS input", default = "/data")
    parser.add_argument("--output", type = str, help = "HDFS output", default = "/output")
    args = parser.parse_args()

    spark = SparkSession.builder\
                    .appName("wcd_spark_job")\
                    .getOrCreate()

    transform_data(input_loc = args.input, output_loc = args.output)
