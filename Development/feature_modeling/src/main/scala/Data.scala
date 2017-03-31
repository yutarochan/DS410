/*
 * Data Management: Utility Function for Dataset Management
 * Authors: Yuya Ong & Yiyue Zou
 */
package feature_modeling

import org.apache.spark.rdd.RDD

object Data extends Serializable {
    def reviewData()

    def parseData(lines: RDD[String]): RDD[Map[String, Array[String]]] = {
        val rows = lines.map(line => line.split(','))
        val papers = rows.map(rowToPaper)
        return papers.filter(m => m.contains("A"))
    }
}
