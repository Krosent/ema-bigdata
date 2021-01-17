import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]): Unit = {

    println("Hello, world!")

    val conf = new SparkConf()
    conf.setAppName("0561433")
    conf.setMaster("local[2]")
    val session = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    val ds: RDD[Float] = session.read.textFile("dataset-sample.txt").map(el => el.toFloat).rdd.persist()

    println("Pop mean: " + mean(ds))

    println("Pop variance: " + variance(ds, mean(ds)))

    println("num of lines: " + ds.count())
  }

  type GMM = (Array[Float], Array[Float], Array[Float])

  def EM(X: RDD[Float], K: Int): Unit = {
    // Initialization Step
    //val sampling: Array[Float] = sample(X, K)

    val rddMean: Float = mean(X)
    val rddVariance: Float = variance(X, rddMean)
    val varianceBar: Array[Float] = Array.fill(K)(rddVariance)
    val phiBar: Array[Float] = Array.fill(K)(1/K)
  }

  /*def sample(X: RDD[Float], K: Int): Array[Float] = {
    // X.sam
  } */

  // def logLikelihood(X:RDD[Float], PhiBar:Array[Float], )

  def mean(X: RDD[Float]): Float = {
    // Mean equals a sum of all values divided on the number of elements
    X.fold(0)((fst, snd) => fst + snd) / X.count()
  }

  def variance(X: RDD[Float], datasetMean: Float): Float = {
    // Formula => SUM( (x (value from sample) - dataset mean)^2 ) / N (population size)

    val datasetSize = X.count()

    (X.map(el => Math.pow(el - datasetMean, 2))
      .reduce((fst, snd) => fst + snd) / datasetSize).toFloat
  }

  /* def EM(X: RDD[Float], K: Int): GMM = {
    return()
  } */
}