package com.viacom18.CMS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.control.NonFatal
object MonthlyViewersShow {

  def main(args: Array[String]) {

 val file1 =args(0) //.."/cms/mviewers/1"
 val file2 =args(1) //.."/cms/mviewers/2"


try{
  val spark = SparkSession.builder.appName("CMSMonthlyViewersShow").enableHiveSupport().getOrCreate()

  val inputDF1 = spark.sql("SELECT ltrim(rtrim(upper(SHOW))) as SHOW, upper(SBU) SBU,upper(GENRE) GENRE, Upper(LANGUAGE) LANGUAGE,Upper(CLUSTER)CLUSTER, DATE_FORMAT(CONCAT(MONTH_NAME,'-','01'), 'yyyy-MM') MONTH_NAME, SUM(NUM_VIEWERS) VIEWERS FROM CMS_VIEWERS_MONTHLY GROUP BY PROJECT, ltrim(rtrim(upper(SHOW))),upper(SBU),upper(GENRE),upper(LANGUAGE),upper(CLUSTER), MONTH_NAME")
  inputDF1.createOrReplaceTempView("inputDF1")
  val cnt_mapper_id=spark.sql ("select ltrim(rtrim(upper(ref_Series_title))) as ref_Series_title , count(distinct id) as id from content_mapper  group  by ltrim(rtrim(upper(ref_Series_title)))")
  cnt_mapper_id.createOrReplaceTempView("cnt_mapper_id")
  val inputDF2 = spark.sql("SELECT ltrim(rtrim(upper(st.SHOW))) as SHOW, upper(st.SBU) SBU, upper(st.GENRE) GENRE , upper(st.LANGUAGE) LANGUAGE, upper(st.CLUSTER) CLUSTER, cast(sum(cm.id)/count(cm.id) as int) as MEDIA_ID_COUNT,SUM(st.NUM_VIEWERS) TOTAL FROM CMS_VIEWERS_MONTHLY_SIDETOTAL2 st left join cnt_mapper_id cm on ltrim(rtrim(upper(st.show))) = ltrim(rtrim(upper(cm.ref_Series_title))) GROUP BY ltrim(rtrim(upper(st.SHOW))),upper( st.SBU), upper(st.GENRE), upper(st.LANGUAGE),upper( st.CLUSTER)")
  inputDF2.createOrReplaceTempView("inputDF2")



  val joinedDF =spark.sql(" select  upper(b.SHOW) as SHOW,upper(a.SBU)SBU,upper(a.GENRE)GENRE, upper(a.LANGUAGE)LANGUAGE,upper(a.CLUSTER)CLUSTER,b.MEDIA_ID_COUNT,b.TOTAL,a.MONTH_NAME,sum(a.VIEWERS) as VIEWERS from inputDF1 a left join inputDF2 b on upper(a.SHOW)= upper(b.SHOW) and  upper(a.SBU) = upper(b.SBU) and upper(a.GENRE) = upper(b.GENRE) and upper(a.LANGUAGE) = upper(b.LANGUAGE) and upper(a.CLUSTER) =upper(b.CLUSTER)   group by upper(b.SHOW),upper(a.SBU),upper(a.GENRE),upper(a.LANGUAGE),upper(a.CLUSTER),b.MEDIA_ID_COUNT,b.TOTAL,a.MONTH_NAME ")
  val pivotDF1 = joinedDF.groupBy("SHOW", "SBU", "GENRE","LANGUAGE", "CLUSTER", "MEDIA_ID_COUNT", "TOTAL").pivot("MONTH_NAME").sum("VIEWERS").na.fill(0)
  val colsToSort = pivotDF1.columns.lastOption.mkString
  val finalDF = pivotDF1.orderBy(col(colsToSort).desc)
  // save the pivotDF1 output
  finalDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(file1)
  val inputDF3 = spark.sql("SELECT '' SHOW, '' SBU, '' GENRE, '' LANGUAGE, '' CLUSTER, '' MEDIA_ID_COUNT, '' TOTAL, DATE_FORMAT(CONCAT(MONTH_NAME,'-','01'), 'yyyy-MM') MONTH_NAME, NUM_VIEWERS VIEWERS FROM CMS_VIEWERS_MONTHLY_UPPERTOTAL1 where DATE_FORMAT(CONCAT(MONTH_NAME,'-','01'), 'yyyy-MM') not in('2017-04') ")
  // apply logic
  val pivotDF2 = inputDF3.groupBy("SHOW", "SBU", "GENRE", "LANGUAGE", "CLUSTER", "MEDIA_ID_COUNT", "TOTAL").pivot("MONTH_NAME").sum("VIEWERS").na.fill(0)
  // save the pivotDF2 output
  pivotDF2.coalesce(1).write.format("csv").mode("overwrite").option("header", "false").save(file2)
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
