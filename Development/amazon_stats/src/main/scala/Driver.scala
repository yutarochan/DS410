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

import scala.util.parsing.json._

object AmazonStats {
    // Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "amazon_stats"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

    // Review Object
    /*
    // TODO: Refactor markup type of object and implement case class
    [JSON SCHEMA]
    Map(
        reviewTime -> 12 16, 2012,
        reviewerID -> A10000012B7CGYKOMPQ4L,
        asin -> 000100039X,
        reviewText -> ...,
        reviewerName -> Adam,
        unixReviewTime -> 1.355616E9,
        helpful -> List(0.0, 0.0),
        overall -> 5.0,
        summary -> Wonderful!
    )
    */

    /*
    // TODO: Refactor and move below source to separate file.
    case class Review(name: String, red: Int, green: Int, blue: Int)
    object JSONProtocol extends DefaultJsonProtocol {
        implicit val reviewFormat = jsonFormat4(Review)
    }
    See tutorial for http://blog.scottlogic.com/2013/07/29/spark-stream-analysis.html
    */

    def main(args: Array[String]): Unit = {
        // Configure SparkContext
		val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
		val sc = new SparkContext(conf)

        // Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);

        // Import HDFS and Parse JSON Object
        val lines = sc.textFile("hdfs:/user/yjo5006/reviews_Books_5.json.gz")

        // val json = lines.map(x => if (JSON.parseFull(x) != None) JSON.parseFull(x).get)


    }
}
