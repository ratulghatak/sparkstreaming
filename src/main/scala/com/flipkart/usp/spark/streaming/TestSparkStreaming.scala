package com.flipkart.usp.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}


/**
  * Created by ayan.ghatak on 25/07/17.
  */
object TestSparkStreaming extends App{

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val sparkConf = new SparkConf().setAppName("Test").setMaster("local[4]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  val topics = Array("topicA")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  val x =stream.map(record => (record.key, record.value))
  stream.foreachRDD(rdd => {
    //rdd.foreach(println)
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    rdd.foreachPartition { iter =>
      val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    }
  })

  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate
}
