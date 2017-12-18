import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer;
import java.io._

import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.mutable.HashMap

object HW5 {
  def main(args: Array[String]) {

  	val sparkConf = new SparkConf().setAppName("HW5")
  	val sc = new SparkContext(sparkConf)

val textFile = sc.textFile(args(0)) //sc.textFile("/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW3/ml-latest-small/ratings.csv")
var header = textFile.first() 
var data = textFile.filter(row => row != header) 
var ratings_rdd = data.map(line => line.split(",")).map(x => (x(0).toInt, x(1).toInt))//.map(x => ((x._1._1+x._1._2), x._2))
var all_users = ratings_rdd.groupByKey().mapValues(x => x.toList).map(x => (1,x))

var reqd_pairs = all_users.join(all_users).values.filter(x => (x._1._1 < x._2._1)).map(x => (List((x._1._1, x._2._1)):::List((x._2._1, x._1._1)), (x._1._2.intersect(x._2._2)))).filter(x => x._2.length >= 3).map(_.swap).flatMapValues(x => x).values

var neighbors = reqd_pairs.groupByKey().mapValues(_.toList).collect.toList
//var neighList = reqd_pairs.collect.toList

var allVertices = ratings_rdd.keys.distinct
allVertices.cache()



def bfs_between(n: Int) : List[((Int, Int), Double)] = {
var nEdges = neighbors.filter(_._1==n)
var current = nEdges.map(_._2).flatten 
var visited = current ++ List(n)
do{
var tempEdges = neighbors.filter(x => (current contains x._1)).map(x => (x._1, x._2 diff visited))
current = tempEdges.map(_._2).flatten.distinct
visited ++:=  current
nEdges ++= tempEdges
}while (current.nonEmpty)
var newEdges = nEdges.flatMap(x => x._2.map(y => (x._1, y)))
var parentEdges :HashMap[Int, List[Int]]= HashMap.empty[Int, List[Int]] ++ newEdges.groupBy(_._2).mapValues(l => l.map(_._1).toList).toMap
var weightEdges :HashMap[(Int, Int), Double]= HashMap.empty[(Int, Int), Double]
var weightVerts :HashMap[Int, Double] = HashMap(visited.map(x => (x, 1.0)).toMap.toSeq:_*)
visited = visited.dropRight(1)
for (v <- visited){
var partialW = weightVerts(v)/parentEdges(v).length
for (c <- parentEdges(v)){
weightEdges((c, v)) = partialW
weightVerts(c) = weightVerts(c) + partialW
}
}
return weightEdges.toList
}

var allBetween = allVertices.flatMap(bfs_between).reduceByKey(_+_).map(x => (((math floor x._2 * 100000) / 100000), x._1)).sortByKey(false).map(_.swap)

allBetween.sortByKey().map(x => (x._1, (math floor x._2 * 10) / 10)).map(x => List(x._1._1.toString, x._1._2.toString, x._2)).map(x =>"(" + x.mkString(",") + ")").coalesce(1, true).saveAsTextFile(args(2))

var betweenMap : HashMap[Double, List[(Int, Int)]] = HashMap(allBetween.filter(x => x._1._1 < x._1._2).map(x => ((List(x._1):::List((x._1._2, x._1._1))), x._2)).map(_.swap).flatMapValues(x => x).groupByKey().mapValues(_.toList).collect.toMap.toSeq:_*)

var edgesMap :HashMap[(Int, Int), Double] = HashMap(allBetween.map(x => ((x._1._1, x._1._2), 1.0)).collect.toMap.toSeq:_*)

var neighborsHashMap :HashMap[Int, Int] = HashMap(neighbors.toMap.map(x => (x._1, x._2.length)).toSeq:_*)

val edges = allBetween.map(x => (Edge(x._1._1, x._1._2, x._2)))
var g = Graph.fromEdges(edges, defaultValue = 0.0)





// First Modularity
var allV = neighborsHashMap.keys.toList
var mod : Double = 0.0
for (i <- allV){
for (j <- allV){
if (i < j){
mod += edgesMap.getOrElse((i,j), 0.0) - (1.0*(neighborsHashMap(i) * neighborsHashMap(j))/edgesMap.size)
}
}
}
var modularity = mod*2/edgesMap.size
var removing_edges = betweenMap.filterKeys(_ > 6.185).values.toList.flatten
edgesMap = edgesMap.filter(t => (!(removing_edges contains t._1)))
var remove_count = sc.parallelize(removing_edges).groupByKey().mapValues(_.toList.length).collect
for (p <- remove_count){
neighborsHashMap(p._1) = neighborsHashMap(p._1) - p._2
}
g = g.subgraph(epred = e => (!(removing_edges contains (e.srcId, e.dstId))))
betweenMap = betweenMap.filter(t => t._1 <=6.185)
// Modularity Loop
var old_mod = modularity
var new_mod = modularity
var max_bet = 0.0
while (old_mod <= new_mod){
old_mod = new_mod
max_bet = betweenMap.keys.max
var removing_edges = betweenMap(max_bet)
//var removing_edges = betweenMap.filterKeys(_ > 6.2).values.toList.flatten
for (e <- removing_edges){
neighborsHashMap(e._1) = neighborsHashMap(e._1) - 1
edgesMap = edgesMap - e
}
betweenMap = betweenMap - max_bet
g = g.subgraph(epred = e => (!(removing_edges contains (e.srcId, e.dstId))))
var comm = HashMap(g.connectedComponents.vertices.collect.toList.map(x => (x._1.toInt, x._2.toInt)).toMap.toSeq:_*)
allV = neighborsHashMap.keys.toList
mod = 0.0
if (max_bet < 6.18444){ mod = -1.0}
for (i <- allV){
for (j <- allV){
if (i<j){
if (comm(i) == comm(j)){
mod += edgesMap.getOrElse((i,j), 0.0) - (1.0*(neighborsHashMap(i) * neighborsHashMap(j))/edgesMap.size)
}
}
}
}
new_mod = mod*2/edgesMap.size
}
var op = "["
val writer = new PrintWriter(new File(args(1)))
var all = g.connectedComponents().vertices.sortByKey().keys.collect.toList
for (p <- all) { op = op.concat(p+",")}
op = op.dropRight(1)
op = op.concat("]")
writer.write(op)
writer.close()

}
}

//./bin/spark-submit --driver-memory 8g --class "HW5" --master local[4] /Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW5/HW5/target/scala-2.10/hw5_2.10-1.0.jar "/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW5/SaiSree_Kamineni_hw5/Input/ratings.csv" "./SaiSree_Kamineni_communities.txt" "./SaiSree_Kamineni_betweenness.txt"

