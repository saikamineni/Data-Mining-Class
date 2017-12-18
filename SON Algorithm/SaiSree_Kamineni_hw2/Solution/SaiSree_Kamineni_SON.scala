import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
object HW2Task {
  def main(args: Array[String]) {

  	val sparkConf = new SparkConf().setAppName("HW2Task")
  	val sc = new SparkContext(sparkConf)

	
val ratings = sc.textFile(args(1))//"/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW1/ml-1m/ratings.dat")
val users = sc.textFile(args(2))//"/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW1/ml-1m/users.dat")

val basketRDD = if (args(0).toInt == 1) {
val ratingRDD = ratings.map( line => line.split("::") ).map(arr => {(arr(0), arr(1).toInt)})
val userMaleRDD = users.map( line => line.split("::") ).map(arr => {(arr(0), arr(1))}).filter(arr=> {arr._2 == "M"})

userMaleRDD.join(ratingRDD).map(arr => {(arr._1, arr._2._2)}).groupByKey().mapValues(_.toList.sorted).repartition(10)
}
else {

val ratingRDD = ratings.map( line => line.split("::") ).map(arr => {(arr(0).toInt, arr(1).toInt)})
val userFemaleRDD = users.map( line => line.split("::") ).map(arr => {(arr(0).toInt, arr(1))}).filter(arr=> {arr._2 == "F"})

userFemaleRDD.join(ratingRDD).map(arr => {(arr._2._2, arr._1)}).groupByKey().mapValues(_.toList.sorted).repartition(10)

}

val n = args(3).toInt/basketRDD.partitions.length

val task1MapRDD = basketRDD.flatMap {case (key, values) => values.map(v => v -> 1)}.mapPartitions({ x =>
  x.toList
    .groupBy(_._1).mapValues(_.size) // some grouping + reducing the result
    .iterator
}, preservesPartitioning = true).mapPartitions(x => x.filter(_._2 >= n).map(x => x._1->1))

val task1ReduceRDD = task1MapRDD.distinct

val single_items = task1ReduceRDD.map(x=>x._1).collect.toList

val task2MapRDD = basketRDD.flatMap {case (key, values) => values.map(v => v -> 1)}.filter(x => (single_items contains x._1))

val finalItems = task2MapRDD.reduceByKey(_+_).filter(_._2>=args(3).toInt).sortByKey().keys



var op = ""
for (i<- finalItems.collect) yield op = op.concat("("+i.toString+"), ")
op = op.substring(0, op.length-2)


val pairs = finalItems.collect.toList //List(1, 50,...)

val comb_2 = pairs.combinations(2).toList//.collect{ case List(x,y) => (x,y) }

val ip_stage2 = basketRDD.flatMap {case (key, values) => values.map(v => key -> v)}.filter(x => (pairs contains x._2)).groupByKey().mapValues(_.toList.sorted)

val task1MapRDD1 = ip_stage2.flatMap {case (key, values) => values.combinations(2).map(v => v -> 1)}.filter(x => (comb_2 contains x._1)).mapPartitions({ x => x.toList.groupBy(_._1).mapValues(_.size).iterator}, preservesPartitioning = true).mapPartitions(x => x.filter(_._2 >= n).map(x => x._1->1))

val task1ReduceRDD1 = task1MapRDD1.distinct

var double_items = task1ReduceRDD1.map(x=>x._1).collect.toList

val task2MapRDD1 = ip_stage2.flatMap {case (key, values) => values.combinations(2).map(v => v -> 1)}.filter(x => (double_items contains x._1))

var finalItems_2 = task2MapRDD1.reduceByKey(_+_).filter(_._2>=args(3).toInt).keys.distinct.collect.toList.sorted(Ordering[Iterable[Int]])


var it = 3
while (!finalItems_2.isEmpty) {

op = op.concat("\n")
for (i <- finalItems_2) yield op = op.concat("(" + i.mkString(", ") + "), ")
op = op.substring(0, op.length-2)


val triples_all = finalItems_2.flatten.distinct.sorted //List(110, 260, 480,..)

val comb_3_all = triples_all.combinations(it).toList // All possible combinations

var comb_3: List[List[Int]] = List()
var flag = 0
for (comb <- comb_3_all) {
	flag = 0;
	for (i <- comb.combinations(it-1)) {
		if (double_items contains i) {
			flag += 1;
		}
	}
	if (flag == it) {
		comb_3=comb_3:+comb;
		
	}


}

val triples = comb_3.flatten.distinct.sorted

val ip_stage3 = basketRDD.flatMap {case (key, values) => values.map(v => key -> v)}.filter(x => (triples contains x._2)).groupByKey().mapValues(_.toList.sorted)

val task1MapRDD2 = ip_stage3.flatMap {case (key, values) => values.combinations(it).map(v => v -> 1)}.filter(x => (comb_3 contains x._1)).mapPartitions({ x => x.toList.groupBy(_._1).mapValues(_.size).iterator}, preservesPartitioning = true).mapPartitions(x => x.filter(_._2 >= n).map(x => x._1->1))

val task1ReduceRDD2 = task1MapRDD2.distinct

double_items = task1ReduceRDD2.map(x=>x._1).collect.toList

val task2MapRDD2 = ip_stage3.flatMap {case (key, values) => values.combinations(it).map(v => v -> 1)}.filter(x => (double_items contains x._1))

finalItems_2 = task2MapRDD2.reduceByKey(_+_).filter(_._2>=args(3).toInt).keys.distinct.collect.toList.sorted(Ordering[Iterable[Int]])

it += 1;

}

var name = "SaiSree_Kamineni_SON.case"+args(0)+"_"+args(3)+".txt"

new PrintWriter(name) { write(op); close }

}
}
