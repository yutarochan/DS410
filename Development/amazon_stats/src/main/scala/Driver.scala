/*
 * Amazon Dataset: Exploratory Statistics
 * Authors: Yuya Ong & Yiyue Zou
 */
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.functions.{unix_timestamp, from_unixtime}

object AmazonStats {
    // Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "amazon_stats"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

    def main(args: Array[String]): Unit = {
        // Configure SparkContext
		val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
		val sc = new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

        // Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);

        // Import HDFS and Parse JSON Object
        val reviews = sc.textFile("hdfs:/user/yjo5006/reviews_Books_5.json.gz")
		val metadata = sc.textFile("hdfs:/user/yjo5006/meta_Books.json.gz").map(x => x.replace("\'", "\""))

		// Parse JSON and Convert to SparkSQL Dataframe
		val review_df = sqlContext.read.json(reviews)
		val metadata_df = sqlContext.read.json(metadata)

		if(args(0) == "printSchema") {
			// Dataframe JSON Schema
			review_df.printSchema()
			metadata_df.printSchema()
		} else if (args(0) == "reviewer") {
			// Basic Review Statistics
			val reviewers = review_df.select("reviewerID").count
			val reviewers_distinct = review_df.select("reviewerID").distinct.count
			val reviewers_distribution = review_df.groupBy("reviewerID").count().describe()

	        val helpful = review_df.select("helpful").take(10)
	        val helpful_distribution = review_df.groupBy("helpful").count().describe()
	        println(helpful)

			// Temporal Review Analysis
			// val review_date = review_df.select(to_date(from_unixtime(col("unixReviewTime"), "yyyy-MM-dd"))).rdd.map(x=>x.toString)
		}
	}
}
