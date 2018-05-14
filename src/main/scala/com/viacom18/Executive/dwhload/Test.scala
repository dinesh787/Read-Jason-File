package com.viacom18.Executive.dwhload

import org.apache.spark.sql.{SQLContext, SparkSession}

object Test {


  val spark = SparkSession
    .builder()
    .appName("Test")
    .enableHiveSupport()
    .getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]): Unit = {


    val arg1 = args(0)
    val arg2 = args(1)
    println(arg1,arg2)

    val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val count = sqlcontext.sql(s""" select count(*) from  F_Agg_Show_Dly where date_part_col ='$arg1' and project_part_col ='APP'""").take(1)(0).get(0).toString

    val end_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString



    val min = sqlcontext.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString

    sqlcontext.sql(s"""insert into DataTableloadHistory select CURRENT_DATE ,'$arg1' ,'F_Agg_Show_Dly' ,'$arg2','Test', '$count','$st_time','$end_time','$min'  from  DataTableloadHistory  limit 1""")


   val flag = sqlcontext.sql(s"""select count(*) from DataTableloadHistory where Load_type ='Daily' and Project_type ='APP' and Table_Name ='F_Agg_Show_Dly'""").take(1)(0).get(0).toString.toDouble.toInt
   val f= 1;
  if(flag >0 & f >0)
    {
      println("Proceed")
    }
    else
    {
      println("wait")

    }


  }
}
