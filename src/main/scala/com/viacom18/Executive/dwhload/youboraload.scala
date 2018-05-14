package com.viacom18.Executive.dwhload

import org.apache.spark.sql.{SQLContext, SparkSession}

object youboraload {
  val spark = SparkSession
    .builder()
    .appName("youboracontentload")
    .enableHiveSupport()
    //    .config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    //    .config("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
    //    .config("fs.azure.account.key.v18biblobstorprod.blob.core.windows.net", "K1ZcztvuFkq6Hy4P8Uf13kP3yXgXxQJBs/bKZ6Y4cfgzD/9nmjHg9uMwIbAe3ZC7vmCS59Mk/3iloAVOs3zdYQ==")
    .getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]): Unit ={

    val showdlyparq = sqlcontext.read.parquet("wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/DWH_TO_BLOB_F_AGG_YOUBORA_WEB_WT/")

    import org.apache.spark.sql.SaveMode

    showdlyparq.write.mode(SaveMode.Overwrite).saveAsTable("watch_time_inc")

    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    sqlcontext.sql("set hive.exec.dynamic.partition=true")

    sqlcontext.sql("insert into table f_agg_youbora_web_wt partition(date_part_col)  select `col-0`,`col-1`,`col-2`,cast(`col-0` as string) as   date_part_col from  watch_time_inc")

  }
}
