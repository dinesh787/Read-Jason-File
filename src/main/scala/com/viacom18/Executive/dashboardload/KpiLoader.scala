package com.viacom18.Executive.dashboardload


import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object KpiLoader {
  val spark = SparkSession.builder().appName("kpiloader").enableHiveSupport().getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {

    powerBiDifference(args(0),args(1))
    mixPanelDifference(args(0),args(1))
    mixPanelPowerBiDifference(args(0),args(1))
  }

  def calDifference = udf((value1: Double, value2: Double) => {
    var yesvalue = value1.toDouble
    var todayvalue=value2.toDouble
    val diff=(((yesvalue-todayvalue)/yesvalue)*100)
    diff
  })

  def powerBiDifference(run_date:String, powerbi_path:String ) = {
    val todayValue = spark.sql(s"select * from powerbikpivalues where date_stamp='$run_date'")
    var yesterdayValue = spark.sql(s"select * from powerbikpivalues where date_stamp=DATE_SUB(CURRENT_DATE,1)")
    for (s <- yesterdayValue.columns)
    {
      if(s!="dashboard")
        yesterdayValue=yesterdayValue.withColumnRenamed(s,s+1)

    }
    val ss = Seq("dashboard")
    var joindf = todayValue.join(yesterdayValue,ss)

    for (s <- todayValue.columns)
    {
      if(s!="dashboard" && s!="date_stamp"&& s!="date_stamp1")
        joindf=joindf.withColumn(s+2,calDifference(col(s+1),col(s))).drop(col(s)).drop(col(s+1)).withColumnRenamed(s+2,s)
    }

    joindf=joindf.select(todayValue.columns.map(col):_*)
    joindf.write.mode("append").csv(powerbi_path)
  }


  def mixPanelDifference(run_date:String, mixpanel_path:String ) = {
    val todayValue = spark.sql(s"select * from mixpanelkpivalues where date_stamp='$run_date'")
    var yesterdayValue = spark.sql(s"select * from mixpanelkpivalues where date_stamp=DATE_SUB(CURRENT_DATE,1)")
    for (s <- yesterdayValue.columns)
    {
      if(s!="dashboard")
        yesterdayValue=yesterdayValue.withColumnRenamed(s,s+1)

    }
    val ss = Seq("dashboard")
    var joindf = todayValue.join(yesterdayValue,ss)

    for (s <- todayValue.columns)
    {
      if(s!="dashboard" && s!="date_stamp"&& s!="date_stamp1")
        joindf=joindf.withColumn(s+2,calDifference(col(s+1),col(s))).drop(col(s)).drop(col(s+1)).withColumnRenamed(s+2,s)
    }

    joindf=joindf.select(todayValue.columns.map(col):_*)
    joindf.write.mode("append").csv(mixpanel_path)
  }

  def mixPanelPowerBiDifference(run_date:String, mixpower_path:String) = {
    val todayValue = spark.sql(s"select * from mixpanelkpivalues where date_stamp='$run_date'")
    var yesterdayValue = spark.sql(s"select * from powerbikpivalues where date_stamp='$run_date'")
    for (s <- yesterdayValue.columns)
    {
      if(s!="date_stamp")
        yesterdayValue=yesterdayValue.withColumnRenamed(s,s+1)

    }
    val ss = Seq("date_stamp")
    var joindf = todayValue.join(yesterdayValue,ss)

    for (s <- todayValue.columns)
    {
      if(s!="dashboard" && s!="date_stamp"&& s!="dashboard1")
        joindf=joindf.withColumn(s+2,calDifference(col(s+1),col(s))).drop(col(s)).drop(col(s+1)).withColumnRenamed(s+2,s)
    }
    joindf.drop("dashboard").withColumn("dashboard",lit("powerbimixpaneldifference"))
    joindf=joindf.select(todayValue.columns.map(col):_*)
    joindf.write.mode("append").csv(mixpower_path)
  }


}
