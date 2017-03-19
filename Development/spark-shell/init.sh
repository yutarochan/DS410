val reviews = sc.textFile("hdfs:/user/yjo5006/reviews_Books_5.json.gz")
val review_df = sqlContext.read.json(reviews)
review_df.printSchema

