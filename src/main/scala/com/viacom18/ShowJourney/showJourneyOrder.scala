package com.viacom18.ShowJourney
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import scala.math.Ordering.Implicits._
object showJourneyOrder {
  val spark = SparkSession.builder().appName("showjourneyapp").enableHiveSupport().getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {

    val date_zero = args(0)
    val date_one = args(1)
    val Load_type = args(2)
    println("show journey start date", date_zero)


    val Start_date = date_zero
    val end_date = date_one
    var current_date = Start_date //sqlcontext.sql(s""" select date_add('$Start_date','1')""").take(1)(0)(0).toString()

    implicit def localDateOrderer: Ordering[LocalDate] = Ordering.by(d => d.toEpochDay)

    var flag = true
    var date_current = LocalDate.parse(current_date)
    var date_end = LocalDate.parse(end_date)
    while (flag) {

      date_current = LocalDate.parse(current_date)
      date_end = LocalDate.parse(end_date)
      if (date_current <= date_end) {

        LoadShowJourney(current_date,Load_type)
        println("Process over for date - ", current_date)
        flag = true
        current_date = sqlcontext.sql(s""" select date_add('$current_date','1')""").take(1)(0)(0).toString()

      }
      else
        {
          flag=false
          println("No date to process")
        }
    }
  }
  def LoadShowJourney(date_zero: String, Load_type: String)=
  {
    val VOOTAPPBASE =sqlcontext.sql(s""" select * from  voot_app_base_event where media_id is not null and date_stamp ='$date_zero'""")
    VOOTAPPBASE.createOrReplaceTempView("VOOTAPPBASE")

    val CONTENT_MAPPER =sqlcontext.sql("select * from CONTENT_MAPPER")
    CONTENT_MAPPER.createOrReplaceTempView("CONTENT_MAPPER")

    val SBU_CHANNEL_MAPPER =sqlcontext.sql("select * from SBU_CHANNEL_MAPPER")
    SBU_CHANNEL_MAPPER.createOrReplaceTempView("SBU_CHANNEL_MAPPER")

    val temp = sqlcontext.sql(s""" SELECT DISTINCT_ID AS USERID, CAST(DATE_STAMP AS DATE) SNAP_DATE,DATE_STAMP,REF_SERIES_TITLE,DATE_IST,TIME,DENSE_RANK() OVER (PARTITION BY DISTINCT_ID,C.GENRE ORDER BY TIME) AS SEQ,C.GENRE AS GENRE FROM VOOTAPPBASE M LEFT JOIN CONTENT_MAPPER C ON C.ID = M.media_id LEFT JOIN SBU_CHANNEL_MAPPER V ON V.SBU= C.SBU WHERE  upper(EVENT) = 'MEDIAREADY' AND DISTINCT_ID  <> '00000000-0000-0000-0000-000000000000' AND DATE_STAMP = '$date_zero'""")
    temp.createOrReplaceTempView("temp")

    val temp1 = sqlcontext.sql("SELECT USERID,SNAP_DATE,DATE_STAMP,REF_SERIES_TITLE,DATE_IST,TIME,SEQ,GENRE FROM temp where SEQ = 1")
    temp1.createOrReplaceTempView("temp1")

    val temp2 = sqlcontext.sql("SELECT REF_SERIES_TITLE,COUNT(*) AS TOTAL_COUNT FROM temp1 GROUP BY REF_SERIES_TITLE ORDER BY COUNT(*) DESC limit 10")
    temp2.createOrReplaceTempView("temp2")

    val temp3 = sqlcontext.sql("SELECT DISTINCT USERID FROM temp1 WHERE REF_SERIES_TITLE IN (SELECT DISTINCT REF_SERIES_TITLE FROM temp2)")
    temp3.createOrReplaceTempView("temp3")

    val FINAL = sqlcontext.sql(s"""SELECT DISTINCT_ID AS USERID,DATE_STAMP AS SNAP_DATE,REF_SERIES_TITLE,DENSE_RANK() OVER (PARTITION BY DISTINCT_ID,C.GENRE ORDER BY  TIME ) AS SEQ,C.GENRE AS genre FROM VOOTAPPBASE M LEFT JOIN CONTENT_MAPPER C ON C.ID = M.media_id LEFT JOIN SBU_CHANNEL_MAPPER V ON V.SBU= C.SBU WHERE upper(EVENT) = 'MEDIAREADY' AND DISTINCT_ID  <> '00000000-0000-0000-0000-000000000000' AND DATE_STAMP = '$date_zero' AND DISTINCT_ID IN (SELECT DISTINCT USERID FROM temp3)""")
    FINAL.createOrReplaceTempView("FINAL")

    val final1 = sqlcontext.sql("SELECT USERID,SNAP_DATE,REF_SERIES_TITLE,SEQ,genre FROM FINAL where SEQ <= 7")
    final1.createOrReplaceTempView("final1")

    val final2 = sqlcontext.sql("SELECT 'App' AS PROJECT,USERID,SNAP_DATE,REF_SERIES_TITLE,SEQ,genre FROM final1")
    final2.createOrReplaceTempView("final2")

    val final2piv = sqlcontext.sql("select PROJECT,USERID,cast(SNAP_DATE as string),REF_SERIES_TITLE,SEQ,genre from  final2").groupBy("PROJECT","USERID","SNAP_DATE","genre").pivot("SEQ").agg(max("REF_SERIES_TITLE"))
    final2piv.createOrReplaceTempView("final2piv")

    sqlcontext.sql("set hive.exec.dynamic.partition=true")
    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    sqlcontext.sql("insert into SHOWJOURNEYORDER_GENRE partition(date_part_col,project_part_col) SELECT PROJECT,USERID,SNAP_DATE,GENRE,CASE WHEN `1` IS NULL AND (`2` IS NOT NULL OR `3` IS NOT NULL OR `4` IS NOT NULL OR `5` IS NOT NULL OR `6` IS NOT NULL OR `7` IS NOT NULL) THEN 'Unknown' ELSE `1` END AS `1`,CASE WHEN `2` IS NULL AND (`3` IS NOT NULL OR `4` IS NOT NULL OR `5` IS NOT NULL OR `6` IS NOT NULL OR `7` IS NOT NULL) THEN 'Unknown' ELSE `2` END AS `2`,CASE WHEN `3` IS NULL AND (`4` IS NOT NULL OR `5` IS NOT NULL OR `6` IS NOT NULL OR `7` IS NOT NULL) THEN 'Unknown' ELSE `3` END AS `3`,CASE WHEN `4` IS NULL AND (`5` IS NOT NULL OR `6` IS NOT NULL OR `7` IS NOT NULL) THEN 'Unknown' ELSE `4` END AS `4`,CASE WHEN `5` IS NULL AND (`6` IS NOT NULL OR `7` IS NOT NULL) THEN 'Unknown' ELSE `5` END AS `5`,CASE WHEN `6` IS NULL AND (`7` IS NOT NULL) THEN 'Unknown' ELSE `6` END AS `6`,`7`,SNAP_DATE as date_part_col,PROJECT as project_part_col FROM final2piv")

    val final2piv1 = sqlcontext.sql("select PROJECT,USERID,cast(SNAP_DATE as string),REF_SERIES_TITLE,SEQ,NULL as GENRE from final2").groupBy("PROJECT","USERID","SNAP_DATE","genre").pivot("SEQ").agg(max("REF_SERIES_TITLE"))
    final2piv1.createOrReplaceTempView("final2piv1")

    sqlcontext.sql("insert into SHOWJOURNEYORDER partition(date_part_col,project_part_col) SELECT PROJECT,USERID,SNAP_DATE,GENRE,CASE WHEN `1` IS NULL AND (`2` IS NOT NULL OR `3` IS NOT NULL OR `4` IS NOT NULL OR `5` IS NOT NULL OR `6` IS NOT NULL OR `7` IS NOT NULL) THEN 'Unknown' ELSE `1` END AS `1`,CASE WHEN `2` IS NULL AND (`3` IS NOT NULL OR `4` IS NOT NULL OR `5` IS NOT NULL OR `6` IS NOT NULL OR `7` IS NOT NULL) THEN 'Unknown' ELSE `2` END AS `2`,CASE WHEN `3` IS NULL AND (`4` IS NOT NULL OR `5` IS NOT NULL OR `6` IS NOT NULL OR `7` IS NOT NULL) THEN 'Unknown' ELSE `3` END AS `3`,CASE WHEN `4` IS NULL AND (`5` IS NOT NULL OR `6` IS NOT NULL OR `7` IS NOT NULL) THEN 'Unknown' ELSE `4` END AS `4`,CASE WHEN `5` IS NULL AND (`6` IS NOT NULL OR `7` IS NOT NULL) THEN 'Unknown' ELSE `5` END AS `5`,CASE WHEN `6` IS NULL AND (`7` IS NOT NULL) THEN 'Unknown' ELSE `6` END AS `6`,`7`,SNAP_DATE as date_part_col,PROJECT as project_part_col FROM final2piv1")


  }
}
