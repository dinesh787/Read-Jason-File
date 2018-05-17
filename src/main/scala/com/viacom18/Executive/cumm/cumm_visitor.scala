package com.viacom18.Executive.cumm


import org.apache.spark.sql.{SQLContext, SparkSession}
object cumm_visitor {

  val spark = SparkSession.builder().appName("cumm_visitor_both").enableHiveSupport().getOrCreate()
  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {

    val date_zero = args(0)
    val Load_type = args(1)
    println("Cumm Visitor start date ", date_zero)
    cumm_visitor(date_zero,Load_type)

  }

  def cumm_visitor(date_zero: String,Load_type :String) = {

    // val Appflag = sqlcontext.sql(s"""select count(*) from DataTableloadHistory where Load_type ='Daily' and Project_type ='APP' and Table_Name ='F_AGG_PRODUCT_DLY_DAY_GRAIN_BOTH' and LoadData_Date='$date_zero' """).take(1)(0).get(0).toString.toDouble.toInt
    // val Webflag = sqlcontext.sql(s"""select count(*) from DataTableloadHistory where Load_type ='Daily' and Project_type ='WEB' and Table_Name ='F_AGG_PRODUCT_DLY_DAY_GRAIN_BOTH' and LoadData_Date='$date_zero' """).take(1)(0).get(0).toString.toDouble.toInt

    // if(Appflag >0 & Webflag>0)
    // {
    val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val daily_install =  sqlcontext.sql(s"""select SUM(NUM_INSTALLS) FROM  F_AGG_PRODUCT_DLY_DAY_GRAIN_BOTH WHERE `DATE` ='$date_zero'""").take(1)(0).mkString
    val daily_visitor  = sqlcontext.sql(s""" select sum(num_visitors) from F_AGG_PRODUCT_DLY_DAY_GRAIN_BOTH where  date_part_col ='$date_zero'  and new_repeat_flag='NEW'""").take(1)(0).mkString
    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")
    val day1 = sqlcontext.sql(s""" select date_sub('$date_zero',1) """).take(1)(0)
    val day11 = day1.get(0).toString
    val prev_daily_visitor =  sqlcontext.sql(s"select `Cumm_TotalUsers` FROM  f_agg_cumm_visitor_installs WHERE `date` = '$day11' limit 1 ").take(1)(0).mkString
    val prev_daily_install =  sqlcontext.sql(s"select `Cumm_GrossInstalls` FROM  f_agg_cumm_visitor_installs WHERE date_part_col = '$day11' limit 1 ").take(1)(0).mkString

    val final_install = (daily_install.toDouble.toInt+prev_daily_install.toInt).toString()
    val final_user  =(daily_visitor.toDouble.toInt+prev_daily_visitor.toInt).toString()


    sqlcontext.sql(s"insert into  f_agg_cumm_visitor_installs partition(date_part_col) select  '$date_zero', '$daily_install','$daily_visitor','$final_user','$final_install', cast ('$date_zero' as string) as date_part_col  from f_agg_cumm_visitor_installs where `date` = `date` limit 1")
    println("inserted into table f_agg_cumm_visitor_installs")


    val end_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val count = sqlcontext.sql(s""" select count(*) from  F_AGG_PRODUCT_DLY_DAY_GRAIN_BOTH where date_part_col ='$date_zero' and project_part_col ='APP'""").take(1)(0).get(0).toString
    val min = sqlcontext.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString

    sqlcontext.sql(s"""insert into default.DataTableloadHistory  partition(date_stamp) select CURRENT_DATE ,'$date_zero' ,'Executive_Cumm' ,'$Load_type','APP', '$count','$st_time','$end_time','$min', 'Yes','cumm_visitor','$date_zero' as date_stamp from  DataTableloadHistory  limit 1""")
    println("inserted into table default.DataTableloadHistory")


    // }


  }
}