package com.kuznetsov

import org.apache.spark.{SparkConf, SparkContext}
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

  var sample: Array[Double] = _
  var varianceBar: Array[Double] = _
  var phiBar: Array[Double] = _

  var sc: SparkContext = _
  var spark: SparkSession = _

  var xCount = 0

  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("0561433")
    // remove this line
    conf.setMaster("local[2]")

    spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    sc = spark.sparkContext


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
      val gm: Array[GammaValue] = gamma(X, phiBar, sample, varianceBar)

      // Maximization Step
      for (k <- 0 until K) {
        updateWeight(gm, k, xCount.toInt)
        updateMean(gm, k, dataPointsNumber = xCount.toInt)
        updateVariance(gm, k)
      }

      lnpCopy = lnpX
      lnpX = logLikelihood(X, phiBar, sample, varianceBar)
      println("lnP(x) value:" + lnpX)

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
            varianceBar: Array[Double]): Array[GammaValue] = {

    // We store our results for each n here. Array buffer is stored since it is mutable data structure.
    var arr: ArrayBuffer[GammaValue] = ArrayBuffer()


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
    X.collect().foreach(datapoint => {
      for(k <- sample.indices) {
        val mean = sample(k)
        val variance = varianceBar(k)
        val covariance = Math.sqrt(variance)

        val numerator = PhiBar(k) * (1 / covariance * Math.sqrt(2 * Math.PI)
          * Math.exp(-(Math.pow(datapoint - mean, 2) / 2 * variance)))

        val gammaObj = new GammaValue(datapoint, k, numerator / denominator)
        arr += gammaObj

      }
    })

    arr.toArray
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

  def updateWeight(gamma: Array[GammaValue], k: Int, dataPointsNumber: Int): Unit = {
    phiBar(k) = gamma.filter(el => el.k == k)
      .map(el => el.value / dataPointsNumber)
      .sum
  }

  def updateMean(gamma: Array[GammaValue], k: Int, dataPointsNumber: Int): Double = {
    val denominator = gamma.filter(el => el.k == k).map(el => el.value).sum
    val num = gamma.filter(el => el.k == k)
      .map(el => el.value * el.n)
      .sum

    sample(k) = num / denominator
    num / denominator
  }

  def updateVariance(gamma: Array[GammaValue], k: Int): Unit = {
    val denominator = gamma.filter(el => el.k == k).map(el => el.value).sum
    val num = gamma.filter(el => el.k == k)
      .map(el => el.value * Math.pow(el.n - sample(k), 2))
      .sum
    varianceBar(k) = num / denominator
  }

  class GammaValue(var n: Double, var k: Int, var value: Double)
}

