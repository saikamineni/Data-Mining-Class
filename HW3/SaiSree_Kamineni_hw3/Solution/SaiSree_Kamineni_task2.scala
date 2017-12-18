import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer;
import java.util.Calendar
import org.apache.log4j.Logger
import org.apache.log4j.Level

object HW3Task2 {
  def main(args: Array[String]) {

  	val sparkConf = new SparkConf().setAppName("HW3Task2")
  	val sc = new SparkContext(sparkConf)


Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)


val start = Calendar.getInstance().getTime()
val textFile = sc.textFile(args(0)) //sc.textFile("/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW3/ml-latest-small/ratings.csv")

var header = textFile.first() 
var data = textFile.filter(row => row != header) 
var ratings_rdd = data.map(line => line.split(",").toList).map(x => ((x(0).toInt, x(1).toInt), x(2).toDouble))//.map(x => ((x._1._1+x._1._2), x._2))


val textFile1 = sc.textFile(args(1)) //sc.textFile("/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW3/testing_small.csv")

val header1 = textFile1.first() 
val data1 = textFile1.filter(row => row != header1) 
var rating_test = data1.map(line => line.split(",").toList).map(x => (x(0).toInt, x(1).toInt))

val ratings_train = ratings_rdd.map(x => x._1).subtract(rating_test.map(x => (x._1, x._2))).map(x => ((x._1, x._2), 0)).join(ratings_rdd).map(x => (x._1, x._2._2))

ratings_train.cache()



// Calculate weights

val users_items = ratings_train.map(x => (x._1._1, List(x._1._2))).reduceByKey( _ ::: _).map(x => (1, x)).repartition(20)

//Array[(List[Int], List[Int])] array(pairusers, pair products)
val co_rated_items = users_items.join(users_items).filter(x => x._2._1._1 < x._2._2._1).map(x => ((List(x._2._1._1) ::: List(x._2._2._1)), x._2._1._2 intersect x._2._2._2)).filter(x => x._2.length>1) 

//Array[((Int, Int), List[Int])] array((user, item), pairusers)
val only_co_rated = co_rated_items.flatMapValues(x => x).map(x => ((x._1, x._2), x._1)).flatMapValues(x => x).map(x => ((x._2, x._1._2), x._1._1))

val intermRDD = ratings_train.join(only_co_rated)

val avg_rating = intermRDD.map(x => ((x._2._2, x._1._1), (x._2._1, 1))).reduceByKey((x, y)=>(x._1+y._1, x._2+y._2)).mapValues(x=>(x._1/x._2))
//
val rat_minus_avg = intermRDD.map(x => ((x._2._2, x._1._1), (x._2._1, x._1._2))).join(avg_rating).map(x => (x._1, (x._2._1._1-x._2._2, x._2._1._2)))

val all_denoms = rat_minus_avg.map(x => (x._1, x._2._1*x._2._1)).reduceByKey(_+_).map(x => (x._1._1, math.sqrt(x._2))).reduceByKey(_*_).filter(_._2 != 0)

val all_nums = rat_minus_avg.map(x => ((x._1._1, x._2._2), x._2._1)).reduceByKey(_*_).map(x => (x._1._1, x._2)).reduceByKey(_+_)

var all_weights = all_nums.join(all_denoms).map(x => (x._1, x._2._1/x._2._2))
all_weights = all_weights.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
all_weights.cache()



// build model
val corated_users = all_weights.flatMap(x => x._1.map(y => (y, (x._2,x._1)))).map(x => (x._1, ( x._2._2.filter(_ != x._1)))).reduceByKey(_ ::: _)

val item_user_list = ratings_train.map(x => (x._1._2, x._1._1)).groupByKey().mapValues(x => x.toList)

val user_avg_rating = ratings_train.map(x => (x._1._1, (x._2,1))).reduceByKey((x,y) => (x._1+y._1, x._2+y._2)).mapValues(x => x._1/x._2)
user_avg_rating.cache()

val user_rating_minus_avg = ratings_train.map(x => (x._1._1, (x._1._2, x._2))).join(user_avg_rating).map(x => ((x._1, x._2._1._1), x._2._1._2 - x._2._2))


var test_5_neighbors = corated_users.join(rating_test).map(x => (x._2._2, (x._1, x._2._1))).join(item_user_list).map(x => ((x._2._1._1, x._1), x._2._1._2.filter(x._2._2.contains(_)).slice(0, 4)))//.flatMapValues(x => x)//.take(10)
test_5_neighbors.cache()

var normal_wts = all_weights.map(x => (x._2,(List((x._1(0),x._1(1))):::List((x._1(1),x._1(0)))))).flatMapValues(x => x).map(_.swap)


// Predictions
//[((Int, Int), ((Int, Int), Double))]
var num1 = test_5_neighbors.flatMapValues(x => x).map(x => ((x._2, x._1._2), x._1)).join(user_rating_minus_avg).map(x => ((x._1._1, x._2._1._1), (x._2))).join(normal_wts).map(x => (x._2._1._1, (x._2._1._2*x._2._2))).reduceByKey(_+_)
var denom1 = test_5_neighbors.flatMapValues(x => x).map(x => ((x._2, x._1._1), x._1)).join(normal_wts).map(_._2).reduceByKey(_+_)

var single_term = user_avg_rating.join(rating_test).map(x => ((x._1, x._2._2), x._2._1))

var predictions = num1.join(denom1).map(x => (x._1, x._2._1/x._2._2)).join(single_term).map(x => (x._1, x._2._1 + x._2._2)).map{case (x,y) => if (y > 5) (x, 5.0) else (x,y)}.map{case (x,y) => if (y < 0) (x,0.0) else (x,y)}.filter(_._2 >= 0)



var final_predictions = rating_test.subtract(predictions.map(_._1)).join(user_avg_rating).map(x => ((x._1, x._2._1), x._2._2)).union(predictions).sortByKey()

val header2 = sc.parallelize(Array("UserId,MovieId,Pred_rating"))

header2.union(final_predictions.map(x => List(x._1._1.toString, x._1._2.toString, x._2)).map(x =>x.mkString(","))).coalesce(1, true).saveAsTextFile("./SaiSree_Kamineni_result_task2.txt")



val end = Calendar.getInstance().getTime()
val diff = (end.getTime()-start.getTime()).toFloat/1000



val rating_pred = ratings_rdd.join(final_predictions)

val error = rating_pred.map { x =>
  val err = (x._2._1 - x._2._2)
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


//./bin/spark-submit --class "HW3Task2" --master local[4] /Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW3/HW3Task2/target/scala-2.10/hw3task2_2.10-1.0.jar  "/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW3/ml-latest-small/ratings.csv" "/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW3/testing_small.csv"

