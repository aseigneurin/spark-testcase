package com.seigneurin.spark.testcase

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

case class IndividualsWithLabel(individuals: String, label: Double)

object ML extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("testcase")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // an RDD of tuples (string describing 2 individuals, array of distances as doubles)
  val all: DataFrame = loadIndividualsWithDistanceFeatures("/features.txt")
//  all.printSchema()
//  all.show()

  // an RDD of tuples (string describing 2 individuals, label as a double)
  val trainingSet: DataFrame = loadIndividualsWithLabels("/labels.txt")
//  trainingSet.printSchema()
//  trainingSet.show()

  // a DataFrame of LabeledPoints
  val trainingData: DataFrame = trainingSet.join(all, trainingSet("individuals") === all("individuals"))
  trainingData.printSchema()
  trainingData.show()


  def loadIndividualsWithDistanceFeatures(filename: String): DataFrame = {
    val fields = StructField("individuals", DataTypes.StringType) :: Range(1, 19).map(i => StructField("f" + i, DataTypes.DoubleType)).toList
    val schema = StructType(fields)

    val path = getClass.getResource(filename).getPath
    val rdd = sc.textFile(path)
      .map(s => s.split("\\|"))
      .map(x => x.zipWithIndex.map(t => if (t._2 == 0) t._1 else t._1.toDouble))
      .map(x => Row(x: _*))

    sqlContext.createDataFrame(rdd, schema)
  }

  def loadIndividualsWithLabels(filename: String): DataFrame = {
    val path = getClass.getResource(filename).getPath
    val rdd = sc.textFile(path)
      .map(s => s.split("\\|"))
      .map(t => IndividualsWithLabel(t(0), parseBoolean(t(1))))

    sqlContext.createDataFrame(rdd)
  }

  def parseBoolean(s: String): Double = {
    if (s == "y") 1 else 0
  }

}
