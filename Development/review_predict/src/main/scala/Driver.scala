import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.storage.StorageLevel

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

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
        val review_df = sqlContext.read.json(reviews).persist()

        // ID to Integer Mapping - Map Between Integer to String ID (vice-versa)
		val user_int = review_df.select("reviewerID").rdd.map(x=> (x(0).toString, 1) ).groupByKey.map(x=>(x._1,x._2.sum)).filter(_._2 > 14).map(x=>x._1).distinct().zipWithUniqueId()
		val prod_int = review_df.select("asin").rdd.map(x=> (x(0).toString,1) ).groupByKey.map(x=>(x._1,x._2.sum)).filter(_._2 > 24).map(x=>x._1).distinct().zipWithUniqueId()

        // Process Integer Mapping
        val ratings = review_df.select("reviewerID", "asin", "overall").rdd.map(x => (x(0).toString, x(1).toString, x(2).toString.toDouble))
        val ratings_user = ratings.keyBy(_._1).join(user_int).map(x => (x._2._1._1, x._2._1._2, x._2._2.toInt, x._2._1._3))                         // (uid_str, pid_str, uid_int, rating)
        val ratings_data = ratings_user.keyBy(_._2).join(prod_int).map(x => (x._2._1._1, x._2._1._2, x._2._1._3.toInt, x._2._2.toInt, x._2._1._4))  // (uid_str, pid_str, uid_str, pid_str, rating)

		review_df.unpersist()

		if(args(0) == "train") {
			println("[TRAINING MODE]")

	        // Split Dataset
	        val splits = ratings_data.randomSplit(Array(0.7, 0.3))
	        // val train = splits(0).map(x => Rating(x._3, x._4, x._5))
			val train = splits(0).map(x => Rating(x._3, x._4, x._5)).persist(StorageLevel.MEMORY_AND_DISK_SER)
	        val test = splits(1).map(x => Rating(x._3, x._4, x._5))

	        // Build the recommendation model using ALS
	        val rank = 10
	        val numIterations = 20
	        val model = ALS.train(train, rank, numIterations, 0.01, 10)

	        // Evaluate the model on rating data
	        val usersProducts = test.map { case Rating(user, product, rate) => (user, product) }
	        val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate) }

	        val ratesAndPreds = test.map { case Rating(user, product, rate) => ((user, product), rate) }.join(predictions)
	        val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
	            val err = (r1 - r2)
	            err * err
	        }.mean()
	        println("Mean Squared Error = " + MSE)

			// Save and load model
			model.save(sc, "amazon_cf_model")
		} else if (args(0) == "test") {
			println("[TESTING MODE]")

			// Load Trained Model
			val sameModel = MatrixFactorizationModel.load(sc, "amazon_cf_model")

			// Load Product Metadata
			val metadata = sc.textFile("hdfs:/user/yjo5006/meta_Books.json.gz").map(x => x.replace("\'", "\""))
			val metadata_df = sqlContext.read.json(metadata).persist(StorageLevel.MEMORY_AND_DISK_SER)

			// Process Prediction
			val user_pred = Array("A18B0T2O25SFT9","AAX4K7QPDTT20", "AJT9NDFFCC5M9", "A1I0KKPLFSD5TB", "A3COJUSKEDTGJ6")
			for (user <- user_pred) {
				val user_record = review_df.select("reviewerID", "asin", "overall").where("reviewerID = " + user)
				val user_x = user_record.map { case Rating(user, product, rate) => (user, product) }

				// Display Records
				println("[USER: "+user+"]\n")
				println("<Past Purchase Records>")
				for (rec <- user_record.collect() ) {

				}
			}
		}
    }
}
