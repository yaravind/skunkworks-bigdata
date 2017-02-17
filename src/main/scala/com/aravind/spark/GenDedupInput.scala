package com.aravind.spark

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object GenDedupInput {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("{outputPath} {numberOfRecords} {numberOfUniqueRecords}")
      return
    }

    //The output file that will hold the data
    val outputPath = new Path(args(0))

    //Number of records to be written to the file
    val numberOfRecords = args(1).toInt

    //Number of unique primary keys
    val numberOfUniqueRecords = args(2).toInt

    //Open fileSystem to HDFS
    val fileSystem = FileSystem.get(new Configuration())

    //Create buffered writer
    val writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(outputPath)))

    val r = new Random()

    //This loop will write out all the record
    //  Every primary key will get about
    //  numberOfRecords/numberOfUniqueRecords records
    for (i <- 0 until numberOfRecords) {
      val uniqueId = r.nextInt(numberOfUniqueRecords)

      //Format: {key}, {timeStamp}, {value}
      writer.write(uniqueId + "," + i + "," + r.nextInt(10000))
      writer.newLine()
    }

    writer.close()
  }
}