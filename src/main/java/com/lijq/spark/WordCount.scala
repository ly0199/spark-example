package com.lijq.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession

/**
  * doc/chinese_data.txt的格式为每一行一个json对象，里面包含一个字段是分词的content_words
  * 在里面取1条或多条的 content_words 数据就可以统计词频了
  *
  * @author Lijq 2017/12/27
  */
object WordCount {


  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate()
    val df = session.read.text("doc/chinese_data.txt")

    import session.implicits._
    val txtRdd = df.map(row => {
      JSON.parseObject(row.getAs[String]("value")).getString("content_words")
    }).rdd

    println("txtRdd count => " + txtRdd.count())

    val rdd = txtRdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    // 排序取前20量的词语
    rdd.sortBy(_._2, false).take(20).foreach(println)
  }

}
