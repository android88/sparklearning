package com.xwtec.spark

/**
  * Created by Administrator on 2018-4-11.
  */
import org.apache.spark.{SparkContext,SparkConf}
object SparkWordCount {
  def main(args:Array[String]): Unit ={
    val conf=new SparkConf()
    val sc=new SparkContext(conf)
    val text=sc.textFile("hdfs://192.168.40.52:8020/tmp/README.md")
    val result=text.flatMap(_.split(' ')).map((_,1)).reduceByKey(_+_).collect()
    result.foreach(println)
  }
}
