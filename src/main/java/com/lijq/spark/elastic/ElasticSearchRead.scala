package com.lijq.spark.elastic

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  *
  * 读取 ElasticSearch中指定 index 的数据
  *
  * @author Lijq 2017/12/27
  */
object ElasticSearchRead {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("estest").setMaster("local[*]")

    //指定读取的索引名称
    //conf.set("es.resource", "wiseweb_bayou_weibo20171023/bayouweibo20171023")
    conf.set("es.mapping.date.rich", "false")
    conf.set("es.nodes", "10.29.131.25")
    conf.set("es.port", "9200")
    //使用query字符串对结果进行过滤
    conf.set("es.query", " {  \"query\": {    \"match_all\": {    }  }}")
    val sc = new SparkContext(conf)
    val rdd = sc.esRDD("wiseweb_bayou_weibo20171023/bayouweibo20171023")
    rdd.foreach(println)
    println(rdd.count)
  }

}
