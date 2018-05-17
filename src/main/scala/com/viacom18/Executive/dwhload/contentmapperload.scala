package com.viacom18.dwhload


import org.apache.spark.sql.{SQLContext, SparkSession}

object contentmapperload {

  val spark = SparkSession
    .builder()
    .appName("contentmapperloaddata")
    .enableHiveSupport()
    //    .config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    //    .config("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
    //    .config("fs.azure.account.key.v18biblobstorprod.blob.core.windows.net", "K1ZcztvuFkq6Hy4P8Uf13kP3yXgXxQJBs/bKZ6Y4cfgzD/9nmjHg9uMwIbAe3ZC7vmCS59Mk/3iloAVOs3zdYQ==")
    .getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext


  def main(args: Array[String]): Unit ={

    val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString

    val showdlyparq = sqlcontext.read.parquet("wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/DAILY_DWH_TO_BLOB_CONTENT_MAPPER/")

    import org.apache.spark.sql.SaveMode

    showdlyparq.write.mode(SaveMode.Overwrite).saveAsTable("CONTENT_MAPPER_DWH_inc")
    val end_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val count = showdlyparq.count.toInt
    val min = sqlcontext.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString
    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")
    sqlcontext.sql("insert into table content_Mapper select * from  CONTENT_MAPPER_DWH_inc")
    sqlcontext.sql(s"""insert into default.DataTableloadHistory partition(date_stamp) select CURRENT_DATE ,DATE_SUB(CURRENT_DATE,1) ,'conent_mapper' ,'Daily','content', '$count','$st_time','$end_time','$min', 'yes', 'conent_mapper',DATE_SUB(CURRENT_DATE,1) as date_stamp from  DataTableloadHistory  limit 1""")

  }

}
