import scala.io.Source
import scala.collection.mutable.PriorityQueue
import math._
import scala.collection.mutable.ArrayBuffer
import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HW4 {
  def main(args: Array[String]) {

  	val sparkConf = new SparkConf().setAppName("HW4")
  	val sc = new SparkContext(sparkConf)
  	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  	import sqlContext.implicits._

def distance(p1: Array[Double], p2: Array[Double]) = {
  sqrt((p1 zip p2).map { case (x,y) => pow(y - x, 2) }.sum)
}


var input = (for (line <- Source.fromFile(args(0)).getLines) yield line.split(',')).toList.filterNot(_.length < 5)

val k = args(1).toInt


// redefining the points by putting into Array
var input_new = List[Array[Array[String]]]()
for (i <- input) {input_new = input_new ++ List(Array(i))}




def diff(t2: (Array[Array[String]], Array[Array[String]])) = { var c1 = Array(0.0, 0.0, 0.0, 0.0)
var co1 = 0
var c2 = Array(0.0, 0.0, 0.0, 0.0)
var co2 = 0
for (t <- t2._1) {  c1 = c1.zip(t.slice(0,4).map(_.toDouble)).map { case (x, y) => x + y }; co1 += 1}
for (t <- t2._2) {  c2 = c2.zip(t.slice(0,4).map(_.toDouble)).map { case (x, y) => x + y }; co2 += 1}
-distance(c1.map(_/co1), c2.map(_/co2))}

while (input_new.length > k) {
var x = new PriorityQueue[(Array[Array[String]], Array[Array[String]])]()(Ordering.by(diff))

for (i <- input_new.combinations(2).toList) { x.enqueue(i(0) -> i(1)) }

var oldC = x.dequeue
input_new = input_new.filter(_ != oldC._1)
input_new = input_new.filter(_ != oldC._2)

input_new = input_new ++ List(oldC._1 ++ oldC._2)
}


var op = ""
var misMatch = 0
var listC = List[(String, Int)]()
for (n <- List.range(0, k)) {
var c1 = List[String]()
for (i <- input_new(n)) { c1 = c1 ++ List(i(4)) }
listC = c1.groupBy(identity).mapValues(_.size).toList.sorted
op = op.concat("cluster:"+listC(0)._1 + "\n")
for (i <- input_new(n)) yield op = op.concat("[" + i(0) + ", "+ i(1) + ", " + i(2) + ", " + i(3) + ", '" + i(4) + "']\n");
op = op.concat("Number of points in this cluster:" + c1.length + "\n\n")
misMatch += c1.length - listC(0)._2
}
op = op.concat("Number of points wrongly assigned:" + misMatch )
val writer = new PrintWriter(new File("SaiSree_Kamineni_Output_"+k.toString+".txt" ))

writer.write(op)
writer.close()

}
}

//./bin/spark-submit --class "HW4" --master local[4] /Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW4/HW4/target/scala-2.10/hw4_2.10-1.0.jar "/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW4/iris.data.txt" 3

