import org.apache.spark.{SparkConf, SparkContext}
import scala.io.StdIn.readLine

object Main {
  def main(args: Array[String]): Unit = {

    // Configuration and read documents
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RDD project")
    val sc = new SparkContext(conf)
    val documents = sc.wholeTextFiles("src/documents")

    // First require : create this format (Word, Count(Word), Documents_list)
    val RDD = documents
      .map(f => (f._2.split("\\W+").toList.distinct, f._1.split('/').last.split('.')(0)))
      .flatMap(f => f._1.map(x => (x, Set(f._2))))
      .reduceByKey((x, y) => x ++ y)
      .map(f => (f._1, f._2.count(z => true), f._2.mkString(",")))
      .sortBy(r => (r._1, r._2))
      .map(f => {s"${f._1},${f._2} ,${f._3}"})

    // Save result in text file
    RDD.saveAsTextFile("src/wholeInvertedIndex.txt")

    // To input the words to search in text file
    val words = readLine("Enter the words: \n")
      .split(" ")

    //Second require : search operation
    val result = sc.textFile("src/wholeInvertedIndex.txt")
    val search = result
      .map(x => (x.split(",")(0), x.split(",", 3)(2)))
      .map(x => (x._1, x._2.split(",").toSet))
      .filter(x => words.exists(e => x._1.contains(e)))
      .map(x => x._2)
      .reduce((x, y) => x.intersect(y))

    println("The search result")
    search.foreach(println)
  }
}
