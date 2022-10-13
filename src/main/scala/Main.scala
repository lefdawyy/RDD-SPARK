import org.apache.spark.{SparkConf, SparkContext}
import scala.io.StdIn.readLine

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RDD project")
    val sc = new SparkContext(conf)
    val documents = sc.wholeTextFiles("src/documents")
    val RDD = documents
      .map(f => (f._2, f._1.split('/').last.split('.')(0)))
      .flatMap(f => f._1.split(Array(' ', '\n')).map(x => (x, Set(f._2))))
      .reduceByKey((x, y) => x ++ y)
      .map(f => (f._1, f._2.count(z => true), f._2.mkString(",")))
      .sortBy(r => (r._1, r._2))
      .map(f => {
        s"${f._1},${f._2} ,${f._3}"
      })
    RDD.saveAsTextFile("src/wholeInvertedIndex.txt")

    val words = readLine("Enter the words: \n")
      .split(" ")

    val result = sc.textFile("src/wholeInvertedIndex.txt")
    val search = result
      .map(x => (x.split(",")(0), x.split(",", 3)(2)))
      .map(x => (x._1, x._2.split(",").toSet))
      .filter(x => words.exists(e => x._1.contains(e)))
      .map(x => x._2)
      .reduce((x, y) => x.intersect(y))
    println("The result")
    search.foreach(println)
  }
}
