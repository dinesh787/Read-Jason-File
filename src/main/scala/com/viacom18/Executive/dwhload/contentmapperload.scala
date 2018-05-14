package com.viacom18.Executive.dwhload

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

    val showdlyparq = sqlcontext.read.parquet("wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/DAILY_DWH_TO_BLOB_CONTENT_MAPPER/")

    import org.apache.spark.sql.SaveMode

    showdlyparq.write.mode(SaveMode.Overwrite).saveAsTable("CONTENT_MAPPER_DWH_inc")

    sqlcontext.sql("insert into table content_Mapper select * from  CONTENT_MAPPER_DWH_inc")

  }

}
