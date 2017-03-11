/*
 * Review JSON Object Protocol
 * Authors: Yuya Ong & Yiyue Zou
 */
import play.api.libs.json._

case class Review(
    reviewTime: String,
    reviewerID: String,
    asin: String,
    reviewText: String,
    reviewerName: String,
    unixReviewTime: Long,
    helpful: Array(Float, Float),
    overall: Float,
    summary: String
)

object ReviewJSON {
    def serializeJSON(rev: Review) = {
        JsObject(Seq(
            "reviewTime" -> JsString(rev.reviewTime),
            "reviewerID" -> JsString(rev.reviewID),
            "asin" -> JsString(rev.asin),
            "reviewText" -> JsString(rev.reviewText),
            "reviewerName" -> JsString(rev.reviewerName),
            "unixReviewTime" -> JsNumber(rev.unixReviewTime),
            "helpful" -> JsArray(rev.helpful),
            "overall" -> JsNumber(rev.overall),
            "summary" -> JsString(rev.summary)
        ))
    }

    def parseJSON(rev: JsValue) = {
        val reviewTime = (rev \ "reviewTime").as[String]
        val reviewerID = (rev \ "reviewerID").as[String]
        val asin = (rev \ "asin").as[String]
        val reviewText = (rev \ "reviewText").as[String]
        val reviewName = (rev \ "reviewName").as[String]
        val unixReviewTime = (rev \ "unixReviewTime").as[Long]
        val helpful = (rev \ "helpful").as[Array[Float]]
        val overall = (rev \ "overall").as[Float]
        val summary = (rev \ "summary").as[String]

        Review(reviewTime, reviewerID, asin, reviewText, reviewName,
            unixReviewTime, helpful, overall, summary)
    }
}

/*
object ReviewJSONProtocol extends DefaultJsonProtocol {
    implicit val reviewFormat = jsonFormat4(Review)
} */
