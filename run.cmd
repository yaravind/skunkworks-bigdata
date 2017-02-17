rm -rf C:/aravind/spark-output/wordcount

spark-submit --class com.aravind.spark.WordCount ./target/skunkworks-bigdata-1.0.0-SNAPSHOT.jar C:/aravind/sw/go/doc/articles/index.html C:/aravind/spark-output/wordcount

tree C:/aravind/spark-output/wordcount