import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration

object ReviewPredict {
    // Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "review_predict"

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
        val reviews = sc.textFile("hdfs:/user/yjo5006/reviews_Books_5.json.gz")
        val review_df = sqlContext.read.json(reviews)

        // ID to Integer Mapping - Map Between Integer to String ID (vice-versa)
        val user_int = review_df.select("reviewerID").rdd.map(x=>x(0).toString).distinct().zipWithUniqueId()
        val prod_int = review_df.select("asin").rdd.map(x=>x(0).toString).distinct().zipWithUniqueId()

        // Process Integer Mapping
        val ratings = review_df.select("reviewerID", "asin", "overall").rdd.map(x => (x(0).toString, x(1).toString, x(2).toString.toDouble))
        val ratings_user = ratings.keyBy(_._1).join(user_int).map(_)
        // val ratings_data = ratings_user.keyBy(_._2).join(prod_int).map{case (_, (prod, user, rating)) => (prod, user, rating)}

        println(ratings_user)
        // println(ratings_data.take(10))
	}
}
