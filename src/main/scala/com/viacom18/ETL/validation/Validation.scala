package com.viacom18.ETL.validation

import org.apache.spark.sql.{SQLContext, SparkSession}

object Validation {

  val spark = SparkSession.builder().appName("validation").enableHiveSupport().getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext
  def main(args: Array[String])
  {
    
    doValidation(args(0), args(1),args(2))
  }

def doValidation(jobTime: String, date_set: String,load_type:String) = {

  val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString

  var job1:Int=0
  var job2:Int=0
  var job3:Int=0
  var job4:Int=0
  var job5:Int=0
  var job6:Int=0
  var job7:Int=0

if(jobTime.equalsIgnoreCase("SET")) {
job1 = spark.sql(s"select record_count from default.datatableloadhistory where load_type ='$load_type' and UPPER(table_name) = 'ETL' and UPPER(project_type) = 'APP' and job_type='00_04' and date_stamp='$date_set'").head.get(0).toString.toInt

job2 = spark.sql(s"""select record_count from default.datatableloadhistory where load_type ='$load_type' and UPPER(table_name) = 'ETL' and UPPER(project_type) = 'APP' and job_type='04_08' and date_stamp='$date_set'""").head.get(0).toString.toInt

job3 = spark.sql(s"select record_count from default.datatableloadhistory where load_type ='$load_type' and UPPER(table_name) = 'ETL' and UPPER(project_type) = 'APP' and job_type='08_12' and date_stamp='$date_set'").head.get(0).toString.toInt

job4 = spark.sql(s"select record_count from default.datatableloadhistory where load_type ='$load_type' and UPPER(table_name) = 'ETL' and UPPER(project_type) = 'WEB' and job_type='WEB' and date_stamp='$date_set'").head.get(0).toString.toInt

job5 = spark.sql(s"select record_count from default.datatableloadhistory where load_type ='$load_type' and UPPER(table_name) = 'ETL' and UPPER(project_type) = 'APP' and job_type='12_16' and date_stamp='$date_set'").head.get(0).toString.toInt

job6 = spark.sql(s"select record_count from default.datatableloadhistory where load_type ='$load_type' and UPPER(table_name) = 'ETL' and UPPER(project_type) = 'APP' and job_type='16_20'and date_stamp='$date_set'").head.get(0).toString.toInt

job7 = spark.sql(s"select record_count from default.datatableloadhistory where load_type = '$load_type' and UPPER(table_name) = 'ETL' and UPPER(project_type) = 'APP' and job_type='20_24' and date_stamp='$date_set'").head.get(0).toString.toInt
}else{
println("invalid")
}
val midCount = job2 + job1 + job4 + job3 + job5
val totalCount = job7 + job6 + job5 + job4 + job3 + job2 + job1

var status = ""


if(midCount <= job1 || totalCount <= midCount) status = "no" else status ="yes"
val end_time = sqlcontext.sql("select CURRENT_TIMESTAMP").take(1)(0).get(0).toString
val min = sqlcontext.sql(s"SELECT ((unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600)").take(1)(0).get(0).toString.toDouble.toInt.toString

  println(status)
//perform insert to table
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.exec.dynamic.partition=true")
spark.sql(s"""insert into default.datatableloadhistory partition(date_stamp) select CURRENT_DATE,CURRENT_DATE,'$jobTime','$load_type','APP','0','$st_time','$end_time','$min','$status','VALIDATION$jobTime','$date_set' as date_stamp from DataTableloadHistory limit 1""")

  }
}