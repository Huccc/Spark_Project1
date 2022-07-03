package com.huc.handler

import com.huc.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util
import java.util.Date

object DauHandler {
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    // 1.将数据转为KV的格式
    val midWithLogDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(startUpLog => {
      ((startUpLog.mid, startUpLog.logDate), startUpLog)
    })
    // 2.将相同key的数据聚合到一块
    val midWithLogDateToIterLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midWithLogDateToLogDStream.groupByKey()

    // 3.对value按照时间戳进行排序,并且去除第一条
    val midWithLogDateToListLogDStream: DStream[((String, String), List[StartUpLog])] = midWithLogDateToIterLogDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    // 4.将list集合中的数据打散并返回
    val value: DStream[StartUpLog] = midWithLogDateToListLogDStream.flatMap(_._2)
    value
  }

  /**
   * 批次内去重
   *
   * @return
   */


  /**
   * 批次间去重
   *
   * @param startUpLogDStream
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
    //    val value: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {
    //      // 1.创建redis连接
    //      val jedis: Jedis = new Jedis("hadoop102", 6379)
    //
    //      // 2.查询redis中的数据
    //      val redisKey: String = "DAU:" + startUpLog.logDate
    //      val mids: util.Set[String] = jedis.smembers(redisKey)
    //
    //      // 3.将当前批次的mid与之前批次去重过后的mid（redis中查询出来的mid）做对比 ，重复的去掉
    //      val bool: Boolean = mids.contains(startUpLog.mid)
    //
    //      jedis.close()
    //      !bool
    //    })
    //    value

    // 方案二:（优化）在每个分区下获取连接，以减少连接个数
    // 有返回值mapPartition 无返回值foreachPartition
    //    startUpLogDStream.mapPartitions(partition => {
    //      val jedis: Jedis = new Jedis("hadoop102", 6379)
    //      val logs: Iterator[StartUpLog] = partition.filter(startUpLog => {
    //        // 查询redis中的数据
    //        val redisKey: String = "DAU:" + startUpLog.logDate
    //        val mids: util.Set[String] = jedis.smembers(redisKey)
    //
    //        val bool: Boolean = mids.contains(startUpLog.mid)
    //
    //        !bool
    //      })
    //
    //      jedis.close()
    //      logs
    //    })

    // 方案三:优化，在每个批次内获取一次连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      // 1.获取redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      // 2.获取redis中的数据
      val mids: util.Set[String] = jedis.smembers(redisKey)

      // 广播变量可以将dirver端的数据传到executor端
      // 3.将数据广播到executor端
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      val midRDD: RDD[StartUpLog] = rdd.filter(startUplog => {
        !midBC.value.contains(startUplog.mid)
      })

      jedis.close()
      // transform 的返回值是一个rdd
      midRDD
    })
    value
  }

  /**
   * 将数据写入redis
   *
   * @param startUpLogDStream
   * @return
   */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    // 写库操作  不需要返回值  SparkStreaming中的算子
    // RDD 是一个弹性式分布式数据集，一个数据集合，放的是每一个样例类
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        // 创建redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)

        partition.foreach(startUpLog => {
          val redisKey: String = "DAU:" + startUpLog.logDate
          jedis.sadd(redisKey, startUpLog.mid)
        })
        // 关闭连接
        jedis.close()
      })
      rdd
    })
  }


}
