package com.aravind.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark-submit --class com.aravind.spark.WordCount ./target/wordcount-1.0.0-SNAPSHOT.jar C:/aravind/sw/go/doc/articles/index.html
  */
object WordCount {

  def main(arg: Array[String]) {
    System.out.println("creating context")
    val conf = new SparkConf().setAppName("WordCountApp")
    val sc = new SparkContext(conf)

    val pathToFile = "file:///"+arg(0)
val pathToOutput="file:///"+arg(1)
    System.out.println("file paths pathToFile: "+pathToFile)
    System.out.println("file paths pathToOutput: "+pathToOutput)
    val textFile = sc.textFile(pathToFile)
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    val d=counts.collect()
    d.foreach(d=> System.out.println(d))
    //counts.saveAsTextFile(pathToOutput)
  }

}
