/*
 * Product Feature Modeling: Driver
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
	private final val APPLICATION_NAME = "feature_modeling"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

    // Configure SparkContext
    final val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
    final val sc = new SparkContext(conf)
    final val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Configure HDFS
    val configuration = new Configuration();
    configuration.addResource(CORE_SITE_CONFIG_PATH);
    configuration.addResource(HDFS_SITE_CONFIG_PATH);

    def main(args: Array[String]): Unit = {
        // Import HDFS and Parse JSON Object
    	val metadata = sc.textFile("hdfs:/user/yjo5006/meta_Books.json.gz").map(x => x.replace("\'", "\""))
        val metadata_df = sqlContext.read.json(metadata)

        // Setup RDD Data Structure
        val desc = metadata_df.na.drop.select($"asin", $"description").rdd.map(x => (x(0).toString, x(1).toString))

        print(desc.take(1))

        /*
        if (args(0) == "tf-idf") {

        } else if (args(0) == "word2vec") {

        } */
	}
}
