import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


import org.apache.spark.sql.types.{StructType,StructField,IntegerType,TimestampType,DoubleType,StringType};
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import java.util.Calendar
import org.apache.log4j.Logger
import org.apache.log4j.Level



object HW3Task1 {
  def main(args: Array[String]) {

  	val sparkConf = new SparkConf().setAppName("HW3Task1")
  	val sc = new SparkContext(sparkConf)
  	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  	import sqlContext.implicits._

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val start = Calendar.getInstance().getTime()
// Ratings full file
val textFile = sc.textFile(args(0)) // "/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW3/ml-latest-small/ratings.csv"

var header = textFile.first() 
var data = textFile.filter(row => row != header) 
var ratings_rdd = data.map(line => line.split(",").toList).map(x => ((x(0).toInt, x(1).toInt), x(2).toDouble))

// Ratings small file
val textFile1 = sc.textFile(args(1)) // "/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW3/testing_small.csv"

val header1 = textFile1.first() 
val data1 = textFile1.filter(row => row != header1) 
var rating_test = data1.map(line => line.split(",").toList).map(x => (x(0).toInt, x(1).toInt))

val temp_rdd = ratings_rdd.map(x => x._1).subtract(rating_test.map(x => (x._1, x._2))).map(x => ((x._1, x._2), 0)).join(ratings_rdd).map(x => (x._1, x._2._2))
val ratings_train = temp_rdd.map(x => Rating(x._1._1, x._1._2, x._2))





// Build the recommendation model using ALS
val rank = 5
val numIterations = 10
val model = ALS.train(ratings_train, rank, numIterations, 0.01)

var predictions = model.predict(rating_test).map { case Rating(userId, movieId, rating) => ((userId, movieId), rating) }

predictions = predictions.map{case (x,y) => if (y > 5) (x, 5.0) else (x,y)}.map{case (x,y) => if (y < 0) (x,0.0) else (x,y)}.filter(_._2 >= 0)


val user_avg_rating = temp_rdd.map(x => (x._1._1, (x._2,1))).reduceByKey((x,y) => (x._1+y._1, x._2+y._2)).mapValues(x => x._1/x._2)

var final_predictions = rating_test.subtract(predictions.map(_._1)).join(user_avg_rating).map(x => ((x._1, x._2._1), x._2._2)).union(predictions).sortByKey()

val header2 = sc.parallelize(Array("UserId,MovieId,Pred_rating"))

header2.union(final_predictions.sortByKey().map(x => List(x._1._1.toString, x._1._2.toString, x._2)).map(x =>x.mkString(","))).coalesce(1, true).saveAsTextFile("./SaiSree_Kamineni_result_task1.txt")
val end = Calendar.getInstance().getTime()
val diff = (end.getTime()-start.getTime()).toFloat/1000

val rating_pred = ratings_rdd.join(final_predictions)

val error = rating_pred.map { case ((userId, movieId), (r1, r2)) =>
  val err = (r1 - r2)
  err }
val mse = error.map{x=>x*x}.mean()

val n_1 = error.filter(x=>(x < 1)).count
val n_2 = error.filter(x=>(x < 2 && x >= 1)).count
val n_3 = error.filter(x=>(x < 3 && x >= 2)).count
val n_4 = error.filter(x=>(x < 4 && x >= 3)).count
val n_5 = error.filter(x=>(x >= 4)).count

println(">=0 and <1: " + n_1)
println(">=1 and <2: " + n_2)
println(">=2 and <3: " + n_3)
println(">=3 and <4: " + n_4)
println(">=4: " + n_5)


println("RMSE = " + math.sqrt(mse))


println("The total execution time taken is " + diff + " sec")
}
}


//./bin/spark-submit  --class "HW3Task1" --master local[4] /Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW3/HW3Task1/target/scala-2.10/hw3task1_2.10-1.0.jar "/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW3/ml-latest-small/ratings.csv" "/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW3/testing_small.csv"
