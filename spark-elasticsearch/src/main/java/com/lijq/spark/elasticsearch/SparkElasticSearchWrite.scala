package com.lijq.spark.elasticsearch

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * @author Lijq
  */
object SparkElasticSearchWrite {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SparkElasticSearchReader")
      .setMaster("local[*]")
      .set("es.index.auto.create", "true") // 允许自动创建
      .set("es.resource.write", "lijinquan/{media_type}") // 默认值es.resource, es.resource.write用于写入
      .set("es.nodes", "node168") // elastic地址
      .set("es.port", "9200") // elastic端口
      //.set("es.write.operation", "index")
      // 默认index 基于id添加或替换现有数据，会重新索引
      // create添加新数据如果id存在则异常
      // update更新数据，如果未找到则异常
      // upsert存在则更新，不存在则新增合并
      .set("es.mapping.id", "id") // 写入elasticsearch时包含文档字段/属性字段

    val sc = new SparkContext(conf)

    val user1 = Map("media_type" -> "user", "id" -> 1, "name" -> ("name" + 1), "time" -> new Date())
    val book1 = Map("media_type" -> "book", "id" -> 1, "name" -> ("book" + 1), "time" -> new Date())

    sc.makeRDD(Seq(user1, book1)).saveToEs("lijinquan/{media_type}")
  }
}
