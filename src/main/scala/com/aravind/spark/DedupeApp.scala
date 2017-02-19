package com.aravind.spark

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark-submit --class com.aravind.spark.DedupeApp ./skunkworks-bigdata-1.0.0-SNAPSHOT.jar
  */
object DedupeApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DedupeApp")

    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "/tmp/spark-events")

    val sc = new SparkContext(conf)

    val fileToDedupe = "hdfs://localhost:9000/user/spark/skunkworks-bigdata/dedupe-input.txt"
    val dedupedOutputFile = "hdfs://localhost:9000/user/spark/skunkworks-bigdata/dedupe-output"

    val originalRdd = sc.hadoopFile(fileToDedupe, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1)

    /*
    (t._1=0,t._2=0,0,520)
(t._1=8,t._2=1,1,550)
(t._1=16,t._2=2,2,7727)
(t._1=25,t._2=0,3,8512)
(t._1=34,t._2=2,4,6261)

    originalRdd.foreach(t => {
      println("t._1=" + t._1, "t._2=" + t._2)
    })*/

    val totalRecs = sc.longAccumulator("Total records")
    val dupeRecs = sc.longAccumulator("Duplicate records")

    val keyedRdd = originalRdd.map(t => {
      totalRecs.add(1)
      val splits = t._2.toString.split(",")
      (splits(0), (splits(1), splits(2)))
    })

    val reducedRdd = keyedRdd.reduceByKey((a, b) => if (a._1.compareTo(b._1) > 0) a else b)

    reducedRdd.map(t => t._1 + "," + t._2._1 + "," + t._2._2).saveAsTextFile(dedupedOutputFile)
  }
}