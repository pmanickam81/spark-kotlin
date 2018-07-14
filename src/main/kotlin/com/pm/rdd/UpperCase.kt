package com.pm.rdd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext

fun main(args: Array<String>) {
    System.setProperty("hadoop.home.dir", "c:\\hadoop")
    val sparkConf:SparkConf = SparkConf().setAppName("UpperCase").setMaster("local")
    val sparkContext = JavaSparkContext(sparkConf)

    val lowerCaseLines:JavaRDD<String> = sparkContext.textFile("input/uppercase.text")
    val upperCaseLines:JavaRDD<String> = lowerCaseLines.map{it -> it.toUpperCase()}
    upperCaseLines.saveAsTextFile("output/uppercase")
}