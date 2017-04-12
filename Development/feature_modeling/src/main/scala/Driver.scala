/*
 * Product Feature Modeling: Driver Program
 * Authors: Yuya Ong & Yiyue Zou
 */
package feature_modeling

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.functions.{unix_timestamp, from_unixtime}

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object FeatureModeling {
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
        val metadata_df = sqlContext.read.json(metadata).persist()

        // Setup RDD Data Structure
        val desc = metadata_df.select("asin", "description").rdd.map(x => (x(0), x(1))).filter(x => x._1 != null && x._2 != null).map(x => (x._1.toString, x._2.toString))

        // Preprocess Text
        val desc_token = desc.map(x => (x._1, x._2.replaceAll("\\p{Punct}|\\d","").toLowerCase.split(" ").filter(_ != "").toArray))
        val stopwords = sc.broadcast(sc.textFile("stopwords.txt").collect())
        val tokens = desc_token.map(x => (x._1, x._2.filter(!stopwords.value.contains(_))))

		// Generate Tokens
		val tok  = tokens.map(x => x._2.toSeq).persist()

        if (args(0) == "tf-idf") {
            val hashingTF = new HashingTF()
            val tf: RDD[Vector] = hashingTF.transform(tok)
            tf.cache()

            val idf = new IDF(minDocFreq = 3).fit(tf)
            val tfidf: RDD[Vector] = idf.transform(tf)	// Feature Vector of Each Item
        } else if (args(0) == "word2vec") {
			val word2vec = new Word2Vec()
			val model = word2vec.fit(tok) // Feature Vector of Each Item
        }
	}
}
