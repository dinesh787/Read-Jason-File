package com.viacom18.Executive.dwhload


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.col
object youbora_api {

  val spark = SparkSession.builder().appName("youboraplay").enableHiveSupport().getOrCreate()
  // val conf = new SparkConf().setAppName("Simple Application")
  //  val sc = new SparkContext(conf)
  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {

    val date_zero = args(0)
    println("youbora play for date = ", date_zero)
    FetchData(date_zero)
  }
  def FetchData(date_zero: String) = {

    val st_time = spark.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString

    println("youbora play for date = ", date_zero)
    val date_next = sqlcontext.sql(s""" select date_add('$date_zero',1)""").take(1)(0).get(0).toString
    val url = "/viacom18/data?fromDate=" + date_zero + " 00:00:00&toDate=" +date_next  + " 00:00:00&type=all&granularity=day&metrics=hours&limit=50&groupBy=extraparam1&operation=reducedisjoin"
    val urls = "/viacom18/data?fromDate=" + date_zero + "%2000:00:00&toDate=" + date_next + "%2000:00:00&type=all&granularity=day&metrics=hours&limit=50&groupBy=extraparam1&operation=reducedisjoin"
    val unixTime = System.currentTimeMillis()
    val expirationTime = unixTime + 7600000
    val preUrl = url + "&dateToken=" + expirationTime.toString()
    val key = "viacom182016"
    val source = preUrl + key;
    val   token = md5Hash(source);
    val host = "api.youbora.com";
    val finalurl = "https://" + host + urls + "&dateToken=" + expirationTime.toString() + "&token=" + token;
    println(finalurl)
    println("started accessing")
    val result1 = scala.io.Source.fromURL(finalurl).mkString
    val events =  spark.sparkContext.parallelize( result1:: Nil)
    val df = sqlcontext.read.json(events)

    val data = df.withColumn("data", explode(col("data")))
    val s = data.select("data.metrics")

    val flattened = s.withColumn("metrics", explode(col("metrics")))

    val d = flattened.select("metrics.values")

    val r = d.withColumn("values", explode(col("values")))
    val y = r.select("values.extraparam1","values.value")



    y.createOrReplaceTempView("y")

    val val1 = sqlcontext.sql(" select value from y where upper(extraparam1) = 'PWA'")
    val val2 = sqlcontext.sql(" select value from y where upper(extraparam1) = 'WEB'")

    if(val1.count >0 & val2.count > 0)
    {
      val  pwa =  val1.take(1)(0).get(0).toString.toDouble.toInt.toString
      val  web =  val2.take(1)(0).get(0).toString.toDouble.toInt.toString
      println(pwa)
      println(web)
      sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      sqlcontext.sql("set hive.exec.dynamic.partition=true")
      sqlcontext.sql(s"""insert into table f_agg_youbora_web_wt partition(date_part_col)  select '$date_zero','PWA','$pwa',cast('$date_zero' as string) as   date_part_col from  f_agg_youbora_web_wt limit 1 """)
      sqlcontext.sql(s"""insert into table f_agg_youbora_web_wt partition(date_part_col)  select '$date_zero','web','$web',cast('$date_zero' as string) as   date_part_col from  f_agg_youbora_web_wt limit 1 """)
    }
    val count = val1.count.toInt
    val end_time = spark.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val min = spark.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString

    spark.sql(s"""insert into default.datatableloadhistory partition(date_stamp) select CURRENT_DATE ,CURRENT_DATE ,'YOUBORA' ,'Daily','YOUBORA', '$count','$st_time','$end_time','$min','yes','YOUBORA','$date_zero' as date_stamp  from DataTableloadHistory limit 1""")





  }




  def md5Hash(text: String) : String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}

}

