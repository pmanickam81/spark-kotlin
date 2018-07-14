package com.pm.rdd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import java.lang.System.setProperty

fun main(args: Array<String>) {
    setProperty("hadoop.home.dir", "c:\\hadoop")
    val sparkConf:SparkConf = SparkConf().setAppName("WordCountKotlin").setMaster("local")
    val sparkContext = JavaSparkContext(sparkConf)

    val lines:JavaRDD<String> = sparkContext.textFile("input/word_count.text")
    val words:JavaRDD<String> = lines.flatMap{ it.split(" ")}
    val result:Map<String, Long> = words.countByValue()
    println(result.forEach{(k,v) -> println("The Word is $k and the Count is $v")})

}