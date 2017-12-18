//# part - 2
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object HW1Task2 {
  def main(args: Array[String]) {

  	val sparkConf = new SparkConf().setAppName("HW1Task2")
  	val sc = new SparkContext(sparkConf)

val ratings = sc.textFile(args(0))//"/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/week_1/ml-1m/ratings.dat")
val users = sc.textFile(args(1))//"/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/week_1/ml-1m/users.dat")
val movies = sc.textFile(args(2))//"/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/week_1/ml-1m/movies.dat")

val ratingRDD = ratings.map( line => line.split("::") ).map(arr => {(arr(0), (arr(1), arr(2)))})

val userRDD = users.map( line => line.split("::") ).map(arr => {(arr(0), arr(1))})

val movieRDD = movies.map( line => line.split("::") ).map(arr => {(arr(0), (arr(2)))})

val joinedRDD = ratingRDD.join(userRDD).map(x => {(x._2._1._1, (x._2._2, x._2._1._2.toInt))}).join(movieRDD).map(x => {((x._2._2, x._2._1._1),(x._2._1._2.toInt, 1))})

val finalRDD = joinedRDD.reduceByKey((x,y)=>(x._1+y._1, x._2+y._2)).mapValues(x=>(1.0 * x._1/x._2).toFloat).sortByKey()

val final_ = finalRDD.map( x => List(x._1._1, x._1._2, x._2) ).map(x =>x.mkString(","))

final_.coalesce(1, true).saveAsTextFile("./SaiSree_Kamineni_result_task2.txt") 

}
}

