/*
 * Review Object: JSON Processor and Review Object
 * Authors: Yuya Ong & Yiyue Zou
 */
import scala.util.parsing.json._

case class Review(
    reviewTime: (Int, Int, Int),
    reviewerID: String,
    asin: String,
    reviewText: String,
    reviewerName: String,
    unixReviewTime: Long,
    helpful: List(Float, Float),
    overall: Float,
    summary: String
)

object Review {
}
