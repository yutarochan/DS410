package feature_modeling

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object FeatureExtraction extends Serializable {
    def genFV(data: RDD[(String, Array[String])]): RDD[(String, Vector)] = {
        val token_list = data.map(x => x._2)

        val hashingTF = new HashingTF()
        val tfVectors = hashingTF.transform(token_list).cache()
        val idfModel = new IDF().fit(tfVectors)
        idfModel.transform(tfVectors)
    }

    /*
    def constructFeatureVectorsFromPapers(papers: RDD[Map[String, Array[String]]]): RDD[Vector] = {
        val wordsOfPapers = papers.map(getWordsOfPaper)
        return constructFeatureVectors(wordsOfPapers)
    }

    def constructFeatureVectors(wordsOfPapers: RDD[Iterable[String]]): RDD[Vector] = {
        val hashingTF = new HashingTF()
        val tfVectors = hashingTF.transform(wordsOfPapers)
        val idfModel = new IDF().fit(tfVectors)
        val tfidfVectors = idfModel.transform(tfVectors)
        return tfidfVectors
    } */

    /*
    def getWordsOfPaper(paper: Map[String, Array[String]]): Iterable[String] = {
        val abstracts = paper.getOrElse("A", Array())
        val words = abstracts.flatMap(s => s.split("[ ,.;()]").toIterable)  // Note: parameter for split is regex
        val stopWords = List("", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
        return words.filter(w => !stopWords.contains(w))
    } */
}
