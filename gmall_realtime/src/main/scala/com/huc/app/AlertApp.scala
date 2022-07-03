package com.huc.app

import com.alibaba.fastjson.JSON
import com.atguigu.constants.GmallConstants
import com.huc.bean.{CouponAlertInfo, EventLog}
import com.huc.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.util.control.Breaks.{break, breakable}

object AlertApp {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkConf
    val conf: SparkConf = new SparkConf()
      .setAppName("sparkconf")
      .setMaster("local[*]")

    //
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 3 获取Kafka的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    // 4.将数据转为样例类
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToEventLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {

        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        val times: String = sdf.format(new Date(eventLog.ts))

        // 补全时间字段
        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)

        //返回
        (eventLog.mid, eventLog)
      })
    })

    // 5.开启一个5min的窗口，
    val midToEventWithwindowDStream: DStream[(String, EventLog)] = midToEventLogDStream.window(Minutes(5))

    // 6.将同一个窗口
    val midToIterEventLogDStream: DStream[(String, Iterable[EventLog])] = midToEventWithwindowDStream.groupByKey()

    // 7.根据条件筛选日志
    val boolToCouponAlertDStream: DStream[(Boolean, CouponAlertInfo)] = midToIterEventLogDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>

        // 用来存放用户id
        val uids: util.HashSet[String] = new util.HashSet[String]()

        // 用来存放领优惠卷所涉及的商品id
        val itemIds: util.HashSet[String] = new util.HashSet[String]()

        // 用来存放用户所涉及的行为
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        // 用来存放

        // 定义一个标志位用来判断是否有浏览商品行为
        var bool: Boolean = true;


        // 遍历迭代器中的数据
        breakable {
          for (elem <- iter) {
            // 添加用户所涉及到的行为
            events.add(elem.evid)
            if ("clickItem".equals(elem.evid)) {
              // 有浏览商品的行为
              bool = false
              break()
            } else if ("coupon".equals(elem.evid)) {
              // 没有浏览商品行为.但是有领优惠卷行为
              uids.add(elem.uid)
              itemIds.add(elem.itemid)
            }
          }
        }
        // 生成疑似预警日志
        (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))

        //        if (uids.size() >= 3 && bool) {
        //          CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis())
        //        } else {
        //
        //        }

      }
    })

    // 8.生成预警日志
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = boolToCouponAlertDStream.filter(_._1).map(_._2)

    couponAlertInfoDStream.print()

    // 9.将预警日志写入ES中
    couponAlertInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list: List[(String, CouponAlertInfo)] = partition.toList.map(info => {
          (info.mid + info.ts / 1000 / 60, info)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEXNAME + "0726",list)
      })
    })

    // 10.开启任务并阻塞
    ssc.start()

    ssc.awaitTermination()

  }
}
