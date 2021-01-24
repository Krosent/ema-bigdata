package com.kuznetsov

import org.apache.spark.{SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}
import scala.collection.mutable.ArrayBuffer

object Mn {
  def main(args: Array[String]): Unit = {
    val main = new Main()
    main.main(null)
  }
}

class Main extends App {
  var xCount = 0
  var sample: Array[Double] = _
  var varianceBar: Array[Double] = _
  var phiBar: Array[Double] = _

  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("0561433")
    // remove this line
    conf.setMaster("local[2]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    // On testing machine
    //val ds: RDD[Double] = sc.textFile("/data/bigDataSecret/dataset-PUT_SET_SIZE.txt", 4)
    // .map(el => el.toDouble).persist()

    for(a <- 1 until 10) {
      // Locally
      val ds: RDD[Double] = sc.textFile("dataset-mini.txt")
        .map(el => el.toDouble).persist()

      val startTimeMillis = System.currentTimeMillis()

      val em = EM(ds, 3)
      println("EM: " + em._1.mkString("Array(", ", ", ")") + " | " + em._2.mkString("Array(", ", ", ")")
        + " | " + em._3.mkString("Array(", ", ", ")"))

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("Execution time: " + durationSeconds)
      ds.unpersist()
    }
  }

  type GMM = (Array[Double], Array[Double], Array[Double])

  def EM(X: RDD[Double], K: Int): GMM = {
    // Initialization Step
    xCount = X.count().toInt

    val rddMean: Double = mean(X)
    val rddVariance: Double = variance(X, rddMean)

    sample = sampleFunction(X, K)
    varianceBar = Array.fill(K)(rddVariance)
    phiBar = Array.fill(K)(1.0/K)
    var lnpX: Double = logLikelihood(X, phiBar, sample, varianceBar)
    var lnpCopy: Double = lnpX

    do {
      // Expectation Step
      // We store this array in memory because gamma function is used by update functions several times.
      val gm: RDD[Array[Double]] = gamma(X, phiBar, sample, varianceBar).persist()

      // Maximization Step
      for (k <- 0 until K) {
        updateWeight(gm, k, xCount)
        updateMean(gm, X, k, dataPointsNumber = xCount)
        updateVariance(gm, X, k)
      }

      lnpCopy = lnpX
      lnpX = logLikelihood(X, phiBar, sample, varianceBar)
      println("lnP(x) value:" + lnpX)

      gm.unpersist()
    } while((lnpX - lnpCopy) > 80)

    (phiBar, sample, varianceBar)
  }

  def sampleFunction(X: RDD[Double], K: Int): Array[Double] = {
    X.takeSample(withReplacement = false, K)
  }

  def logLikelihood(X:RDD[Double], PhiBar:Array[Double], sample: Array[Double], varianceBar: Array[Double]): Double = {
    X.map(elem => {
      var rightSideSum = 0.0
      for(k <- sample.indices) {
        val mean = sample(k)
        val variance = varianceBar(k)
        val covariance = Math.sqrt(variance)

        val formula = PhiBar(k) * (1 / covariance * Math.sqrt(2 * Math.PI)
          * Math.exp(-(Math.pow(elem - mean, 2) / 2 * variance)))
        rightSideSum = rightSideSum + formula
      }

      Math.log(rightSideSum)
    }).filter(value => value != Double.NegativeInfinity)
      .reduce((fst, snd) => fst + snd)
    /*
      Note:
        We filter out negative infinity values, since while summing we get the whole sum as negative infinity.
        Our values represented as Double. We replaced it from Float, to have better precision, however it does not solve
        the issue with non-negative values.
     */
  }

  // for each n we have k
  def gamma(X:RDD[Double], PhiBar:Array[Double], sample: Array[Double],
            varianceBar: Array[Double]): RDD[Array[Double]] = {
    /*
      Note: First of all, we decided to calculate denominator of expression.
     */
    val denominator: Double = X.map(elem => {
      var rightSideSum: Double = 0.0

      for(k <- sample.indices) {
        val mean = sample(k)
        val variance = varianceBar(k)
        val covariance = Math.sqrt(variance)

        val formula = PhiBar(k) * (1 / covariance * Math.sqrt(2 * Math.PI)
          * Math.exp(-(Math.pow(elem - mean, 2) / 2 * variance)))
        rightSideSum = rightSideSum + formula
      }
      rightSideSum
    }).filter(value => value != Double.NegativeInfinity)
      .reduce((fst, snd) => fst + snd)

    // for each datapoint calculate likelihood
    // We have to collect data from RDD into our system, otherwise we cannot populate array which
    // is outside our foreach branch.
    X.map(datapoint => {
      var arr: ArrayBuffer[Double] = ArrayBuffer()
      for(k <- sample.indices) {
        val mean = sample(k)
        val variance = varianceBar(k)
        val covariance = Math.sqrt(variance)

        val numerator = PhiBar(k) * (1 / covariance * Math.sqrt(2 * Math.PI)
          * Math.exp(-(Math.pow(datapoint - mean, 2) / 2 * variance)))

        arr += numerator / denominator
      }
      arr.toArray
    })
  }

  def mean(X: RDD[Double]): Double = {
    // Mean equals a sum of all values divided on the number of elements
    X.fold(0)((fst, snd) => fst + snd) / xCount
  }

  def variance(X: RDD[Double], datasetMean: Double): Double = {
    // Formula => SUM( (x (value from sample) - dataset mean)^2 ) / N (population size)

    val datasetSize = xCount

    X.map(el => Math.pow(el - datasetMean, 2))
      .reduce((fst, snd) => fst + snd) / datasetSize
  }

  def updateWeight(gamma: RDD[Array[Double]], k: Int, dataPointsNumber: Int): Unit = {
    phiBar(k) = gamma.map(row => row(k)).map(row => row / dataPointsNumber).sum()
  }

  def updateMean(gamma: RDD[Array[Double]], X: RDD[Double], k: Int, dataPointsNumber: Int): Double = {
    // Persistence of this value demonstrates worse results than recalculation
    val zippedRDD = X.zip(gamma)

    val num = zippedRDD.map(elem => {
      elem._1 * elem._2(k)
    }).sum()

    val denominator = zippedRDD.map(elem => {
      elem._2(k)
    }).sum()

    sample(k) = num / denominator
    num / denominator
  }

  def updateVariance(gamma: RDD[Array[Double]], X: RDD[Double], k: Int): Unit = {
    val zippedRDD = X.zip(gamma)
    val sampleK = sample(k)

    val num = zippedRDD.map(elem => {
      elem._2(k) * Math.pow(elem._1 - sampleK, 2)
    }).sum()

    val denominator = zippedRDD.map(elem => {
      elem._2(k)
    }).sum()

    varianceBar(k) = num / denominator
  }
}


