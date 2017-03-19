 = sc.textFile("hdfs:/user/yjo5006/reviews_Books_5.json.gz")
 val review_df = sqlContext.read.json(reviews)
 review_df.printSchema

 val metadata = sc.textFile("hdfs:/user/yjo5006/meta_Books.json.gz")


 //reviewers = 8898041, including repeated reviewer
 val reviewers = review_df.select("reviewerID").count
 
 //distinct reviewers = 603668
 val reviewers_distinct = review_df.select("reviewerID").distinct.count
 //
 //the average # of review for one reviewer is 14
 //that is, every review buys 14 books in average
 val review_average = reviewers/reviewers_distinct
