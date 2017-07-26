package com.flipkart.usp.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ayan.ghatak on 25/07/17.
  */
object Test extends App{
  val sparkConf = new SparkConf().setAppName("Test").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  val rdd= sc.parallelize(Seq(1,2,3))
  rdd.foreach(println)
  println("Hello World")
}
