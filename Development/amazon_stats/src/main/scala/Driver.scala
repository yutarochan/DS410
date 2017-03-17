/*
 * Amazon Dataset: JSON Parsing Tests and Basic Statistics
 * Authors: Yuya Ong & Yiyue Zou
 */
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration

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
        val lines = sc.textFile("hdfs:/user/yjo5006/reviews_Books_5.json.gz")
		val df = sqlContext.read.json(lines)

		// Dataframe JSON Schema
		df.printSchema()
    }
}
