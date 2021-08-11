package com.kuznetsov

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Mn {
  def main(args: Array[String]): Unit = {
    val maximizationApp = new MaximizationApp
    maximizationApp.startAlgorithm()
  }
}

class MaximizationApp {
  var xCount = 0
  var sample: Array[Double] = _
  var varianceBar: Array[Double] = _
  var phiBar: Array[Double] = _

  val conf = new SparkConf()
  conf.setAppName("0561433")
  // remove this line (only for local testing)
  //conf.setMaster("local[4]")

  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  import spark.implicits._

   def startAlgorithm(): Unit = {
    val sc = spark.sparkContext

    // for(a <- 1 until 10) {
      // Locally
//      val ds: RDD[Double] = sc.textFile("dataset-mini.txt")
       // .map(el => el.toDouble).persist()

     val schema = new StructType()
       .add("X",DoubleType, nullable = false)

     val df: DataFrame = spark
       .read
       .option("header", "false")
       .schema(schema)
       .csv("dataset-medium.txt")

     val ds = df.as[Double]

      // On testing machine
      //val ds: RDD[Double] = sc.textFile("/data/bigDataSecret/dataset-big.txt", 4)
       //.map(el => el.toDouble).persist()

      val startTimeMillis = System.currentTimeMillis()

      val em = EM(ds, 3)

      println("EM: " + em._1.mkString("Array(", ", ", ")") + " | " + em._2.mkString("Array(", ", ", ")")
        + " | " + em._3.mkString("Array(", ", ", ")"))

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("Execution time: " + durationSeconds)
      ds.unpersist()
    //}
  }

  type GMM = (Array[Double], Array[Double], Array[Double])

  def EM(X: Dataset[Double], K: Int): GMM = {

    // repartitioning
    X.repartition(32)

    // Statistics variables
    var numberOfForDoWhileIterations = 0


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
      var startTimeStepEM = System.currentTimeMillis()
      // Expectation Step
      // We store this array in memory because gamma function is used by update functions several times.
      println("Before gm")
      val gm: Dataset[Array[Double]] = gamma(X, phiBar, sample, varianceBar).persist()
      println("After gm")
      // Maximization Step
      for (k <- 0 until K) {
        println("Update weight")
        updateWeight(gm, k, xCount)

        println("Update mean")
        updateMean(gm, X, k, dataPointsNumber = xCount)

        println("Update variance")
        updateVariance(gm, X, k)

//        println("Update mean and variance")
//        updateMeanAndVariance(gm, X, K, dataPointsNumber = 4)

        var endTimeStepEM = System.currentTimeMillis()
        println("EM Step execution time: " + (endTimeStepEM - startTimeStepEM))
      }

      lnpCopy = lnpX
      lnpX = logLikelihood(X, phiBar, sample, varianceBar)
      println("lnP(x) value:" + lnpX)

      // We clear the persistence because we no longer use the data and we will calculate gamma again on the next iteration
      gm.unpersist()

      numberOfForDoWhileIterations = numberOfForDoWhileIterations + 1
    } while((lnpX - lnpCopy) > 80)

    println("Number of do iterations is: " + numberOfForDoWhileIterations)

    (phiBar, sample, varianceBar)
  }

  def sampleFunction(X: Dataset[Double], K: Int): Array[Double] = {
    X.take(K)
  }


  def logLikelihood(X:Dataset[Double], PhiBar:Array[Double], sample: Array[Double], varianceBar: Array[Double]): Double = {
    val startTimeLogLikelihood = System.currentTimeMillis()

    val res = X.map(elem => {
      def calculateRightSumValue: Double = {
        var rightSideSum = 0.0
        for (k <- sample.indices) {
          val mean = sample(k)
          val variance = varianceBar(k)
          val covariance = Math.sqrt(variance)

          val formula = PhiBar(k) * (1 / covariance * Math.sqrt(2 * Math.PI)
            * Math.exp(-(Math.pow(elem - mean, 2) / 2 * variance)))
          rightSideSum = rightSideSum + formula
        }
        rightSideSum
      }

      var rightSideSum: Double = calculateRightSumValue
      Math.log(rightSideSum)
    }).filter(value => value != Double.NegativeInfinity)
      .reduce((fst, snd) => fst + snd)
    /*
      Note:
        We filter out negative infinity values, since while summing we get the whole sum as negative infinity.
        Our values represented as Double. We replaced it from Float, to have better precision, however it does not solve
        the issue with non-negative values.
     */

    val endTimeLogLikelihood = System.currentTimeMillis()
    println("Likelihood function execution time: " + (endTimeLogLikelihood - startTimeLogLikelihood))

    res
  }

  // for each n we have k
  def gamma(X:Dataset[Double], PhiBar:Array[Double], sample: Array[Double],
            varianceBar: Array[Double]): Dataset[Array[Double]] = {
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

  def mean(X: Dataset[Double]): Double = {
    // Mean equals a sum of all values divided on the number of elements
    X.reduce((fst, snd) => fst + snd) / xCount
  }

  def variance(X: Dataset[Double], datasetMean: Double): Double = {
    // Formula => SUM( (x (value from sample) - dataset mean)^2 ) / N (population size)

    val datasetSize = xCount

    X.map(el => Math.pow(el - datasetMean, 2))
      .reduce((fst, snd) => fst + snd) / datasetSize
  }

  def updateWeight(gamma: Dataset[Array[Double]], k: Int, dataPointsNumber: Int): Unit = {
    phiBar(k) = gamma.map(row => row(k)).map(row => row / dataPointsNumber)
      .reduce((fst, snd) => fst + snd)
  }

  def updateMean(gamma: Dataset[Array[Double]], X: Dataset[Double], k: Int, dataPointsNumber: Int): Double = {
    val startTimeUpdateMean = System.currentTimeMillis()

    val df0: DataFrame =  X.withColumn("id", monotonically_increasing_id())
    val df1: DataFrame = gamma.withColumn("id", monotonically_increasing_id())
    val resDf: DataFrame = df0.join(df1, "id").drop("id")

    val num: Double = resDf.map(row => row.getDouble(0) * row.getList[Double](1).get(k)).reduce((fst, snd) => fst + snd)
    val denominator: Double = resDf.map(row => row.getList[Double](1).get(k)).reduce((fst, snd) => fst + snd)

    val endTimeUpdateMean = System.currentTimeMillis()
    println("Update mean execution time: " + (endTimeUpdateMean - startTimeUpdateMean))

    sample(k) = num / denominator
    num / denominator
  }

  def updateVariance(gamma: Dataset[Array[Double]], X: Dataset[Double], k: Int): Unit = {
    val startTimeUpdateVariance = System.currentTimeMillis()
    val sampleK = sample(k)
    val df0 =  X.withColumn("id", monotonically_increasing_id())
    val df1 = gamma.withColumn("id", monotonically_increasing_id())
    val resDf = df0.join(df1, "id").drop("id")

    val num = resDf.map (row => {
      row.getList[Double](1).get(k) * Math.pow(row.getDouble(0) - sampleK, 2)
    }).reduce((fst, snd) => fst + snd)

    val denominator = resDf.map(row => {
      row.getList[Double](1).get(k)
    }).reduce((fst, snd) => fst + snd)


    val endTimeUpdateVariance = System.currentTimeMillis()
    println("Update variance execution time: " + (endTimeUpdateVariance - startTimeUpdateVariance))

    varianceBar(k) = num / denominator
  }
}