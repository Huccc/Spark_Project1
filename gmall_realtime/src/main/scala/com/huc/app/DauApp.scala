package com.huc.app

import com.alibaba.fastjson.JSON
import com.atguigu.constants.GmallConstants
import com.huc.bean.StartUpLog
import com.huc.handler.DauHandler
import com.huc.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

import java.text.SimpleDateFormat
import java.util.Date

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkconf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    // TODO 2.创建streamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // TODO 3.消费Kafka数据
    val kakfaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    // TODO 4.将获取到的json格式数据转为样例类，并补全
    val startUpLogDStream: DStream[StartUpLog] = kakfaDStream.mapPartitions(partition => {
      partition.map(record => {
        // 将数据转为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val times: String = sdf.format(new Date(startUpLog.ts))
        // 补全logDate
        startUpLog.logDate = times.split(" ")(0)
        // 补全logHour
        startUpLog.logHour = times.split(" ")(1)
//        println(startUpLog.mid)
        startUpLog
      })
    })
//    startUpLogDStream.print()

    // cache 缓存 小小的优化一下
    startUpLogDStream.cache()

    // TODO 5.做批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    // cache 缓存 小小的优化一下
    filterByRedisDStream.cache()

    // 原始数据条数
    startUpLogDStream.count().print()

    // 经过批次间去重后的数据条数
    filterByRedisDStream.count().print()

    // TODO 6.做批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)

    filterByGroupDStream.count().print()

    // TODO 7.将去重的结果写入redis中
    DauHandler.saveToRedis(filterByGroupDStream)

    // TODO 8.将去重后的明细数据写入hbase中
    filterByGroupDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix(
        "GMALL0726_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })


    // 测试的
    //    // 4.打印kafka中的数据
    //    kakfaDStream.foreachRDD(rdd=>{
    //      rdd.foreachPartition(patition=>{
    //        patition.foreach(record=>{
    //          println(record.value())
    //        })
    //      })
    //    })


    // 开启任务
    ssc.start()
    // 阻塞任务
    ssc.awaitTermination()
  }
}
