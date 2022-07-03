package com.huc.Demo1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object test01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val dataFrame: DataFrame = session.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092")
      .option("subscribe", "eds_source_test")
      .load()

    //    val odf: DataFrame = session.read
    //      .format("jdbc")
    //      .option("url", "jdbc:oracle:thin:@192.168.130.113:1521:testswin")
    //      .option("dbtable", "MSA_DG_DECL.CL_BIZ_CERT_SCHEDULE")
    //      .option("user", "MSA_DG_DECL")
    //      .option("password", "etestpwd")
    //      .load()

    //    val viewFlag: Boolean = session.catalog.tableExists("stu")
    //    if(!viewFlag){
    //      odf.createTempView("stu")
    //    }

    val df = session.sql("select * from stu")

    df.show()

    //    dataFrame.createOrReplaceTempView("test")

    //    session.sql(
    //      """
    //        |select
    //        |  *
    //        |from
    //        |  test
    //        |""".stripMargin).show()

    session.close()
  }

}
