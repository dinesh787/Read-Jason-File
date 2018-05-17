package com.viacom18.CMS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.control.NonFatal
object MonthlyViewersShow {

  def main(args: Array[String]) {

val file1 =args(0) //.."/cms/mviewers/1"
 val file2 =args(1) //.."/cms/mviewers/2"

    val spark = SparkSession.builder.appName("CMSMonthlyViewersShow").enableHiveSupport().getOrCreate()
try{
    val inputDF1 = spark.sql("SELECT SHOW, SBU, GENRE, LANGUAGE, CLUSTER, MEDIA_ID_COUNT, DATE_FORMAT(CONCAT(MONTH_NAME,'-','01'), 'yyyy-MM') MONTH_NAME, SUM(NUM_VIEWERS) VIEWERS FROM CMS_VIEWERS_MONTHLY GROUP BY PROJECT, SHOW, SBU, GENRE, LANGUAGE, CLUSTER, MEDIA_ID_COUNT, MONTH_NAME")
    inputDF1.createOrReplaceTempView("inputDF1")
    val inputDF2 = spark.sql("SELECT SHOW, SBU, GENRE, LANGUAGE, CLUSTER, SUM(NUM_VIEWERS) TOTAL FROM CMS_VIEWERS_MONTHLY_SIDETOTAL GROUP BY SHOW, SBU, GENRE, LANGUAGE, CLUSTER")
    inputDF2.createOrReplaceTempView("inputDF2")
    ///** changes in code

    val joinedDF =spark.sql(" select a.SHOW,a.SBU,a.GENRE,a.LANGUAGE,a.CLUSTER,a.MEDIA_ID_COUNT,b.TOTAL,a.MONTH_NAME,sum(a.VIEWERS) as VIEWERS from inputDF1 a left join inputDF2 b on a.SHOW = b.SHOW and  a.SBU = b.SBU and a.GENRE =b.GENRE and a.LANGUAGE = b.LANGUAGE and a.CLUSTER =b.CLUSTER   group by a.SHOW,a.SBU,a.GENRE,a.LANGUAGE,a.CLUSTER,a.MEDIA_ID_COUNT,b.TOTAL,a.MONTH_NAME ")

    val pivotDF1 = joinedDF.groupBy("SHOW", "SBU", "GENRE", "LANGUAGE", "CLUSTER", "MEDIA_ID_COUNT", "TOTAL").pivot("MONTH_NAME").sum("VIEWERS").na.fill(0)

    val colsToSort = pivotDF1.columns.lastOption.mkString

    val finalDF = pivotDF1.orderBy(col(colsToSort).desc)

    // save the pivotDF1 output
    finalDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(args(0))

    val inputDF3 = spark.sql("SELECT '' SHOW, '' SBU, '' GENRE, '' LANGUAGE, '' CLUSTER, '' MEDIA_ID_COUNT, '' TOTAL, DATE_FORMAT(CONCAT(MONTH_NAME,'-','01'), 'yyyy-MM') MONTH_NAME, NUM_VIEWERS VIEWERS FROM CMS_VIEWERS_MONTHLY_UPPERTOTAL1")

    // apply logic
    val pivotDF2 = inputDF3.groupBy("SHOW", "SBU", "GENRE", "LANGUAGE", "CLUSTER", "MEDIA_ID_COUNT", "TOTAL").pivot("MONTH_NAME").sum("VIEWERS").na.fill(0)

    // save the pivotDF2 output
    pivotDF2.coalesce(1).write.format("csv").mode("overwrite").option("header", "false").save(args(0))

    spark.stop()

} catch   {
  case NonFatal(t) =>
    println("************************-----------ERROR---------***************************")
    println("--")
    println("--")
    println(t.toString)

}

  }
}
