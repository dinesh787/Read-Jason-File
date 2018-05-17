package com.viacom18.ETL.mobile
import java.util.Calendar

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.SQLContext

import scala.util.Try


/**
  * Converts JSON RAW GZ files into PARQUET SNAPPY
  * ETL Extract Json.gz from blob -Transform to consumable Data -Load as Parquet snappy to blob
  *
  *
  */
object VootMobileDataETL {

  val spark = SparkSession.builder().appName("VootMobileDataETL").enableHiveSupport().getOrCreate()
  spark.sql("set spark.sql.caseSensitive=true") // Allows duplicate Columns in DataFrame.
  val sqlcontext: SQLContext = spark.sqlContext


  // Replace empty Strings with null values
  def setEmptyToNull(df: DataFrame): DataFrame = {
    val exprs = df.schema.map { f =>
      f.dataType match {
        case StringType => when(length(col(f.name)) === 0, lit(null: String).cast(StringType)).otherwise(col(f.name)).as(f.name)
        case _ => col(f.name)
      }
    }

    df.select(exprs: _*)
  }

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  def processVootAppData(inputfilename: String, outputfilepath: String, jobtype:String, dateset:String, loadtype:String)  = {
    try {
      val st_time = spark.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    println("processVootAppData ****************start**************", Calendar.getInstance().getTime())
    println(" read  json starting   !!", inputfilename, " ", outputfilepath)

    // Extract Layer
    val vootExtractJsonGzDF = spark.read.json(inputfilename) // Read Json.Gz files..option("mergeSchema", "true")

    println(" read  json completed  !!" + Calendar.getInstance().getTime())

    // Transform Layer
    val expandPropertiesColumns = vootExtractJsonGzDF.select("event", "properties.*" )
    var vootTransformDF = expandPropertiesColumns

      // Add Custom Columns to DataFrame. additional columns other than Columns from Source.
      vootTransformDF = vootTransformDF.withColumn("date_gmt", from_unixtime((col("time") - lit(5.5 * 60 * 60).cast("long"))).cast(StringType)) //  substract (60 min * 60 secs * 5.5 hours) to the timestamp in order to convert this timestamp into UTC/GMT.
      vootTransformDF = vootTransformDF.withColumn("date_ist", from_unixtime((col("time") - lit(5.5 * 60 * 60).cast("long")) + lit(5.5 * 60 * 60).cast("long")).cast(StringType)) //  add (60 min * 60 secs * 5.5 hours) to the timestamp in order to convert GMT timestamp into IST.
      vootTransformDF = vootTransformDF.withColumn("date_stamp", from_unixtime((col("time") - lit(5.5 * 60 * 60).cast("long")) + lit(5.5 * 60 * 60).cast("long"), "yyyy-MM-dd").cast(StringType))
      vootTransformDF = vootTransformDF.withColumn("date_stamp_ist", from_unixtime((col("time") - lit(5.5 * 60 * 60).cast("long")) + lit(5.5 * 60 * 60).cast("long"), "yyyy-MM-dd").cast(StringType))

    //remove dot from column names. As dot is considered as operator by dataframe and will throw error while retreving  it..
    for (renameCol <- vootTransformDF.columns) {
      if (renameCol.contains('.')) {
        val renamedCol = renameCol.replace(".", " ")
        vootTransformDF = vootTransformDF.withColumnRenamed(renameCol, renamedCol)
      }
    }

    // Convert all column to string except complex datatypes. Remove remaining datatype.
    //Complex datatype/binary types are removed as the data comes in multi nested format .
    //We need to again clean the column names and fix duplicates in an infinite fashion. To write to parquet we need to fix all column name and duplicate col name issues.
    //Manual intervention is required for merging duplicate which cannot be automated.
    //Current spark version 2.0.2 doesnt support converting struct to_json function.Spark 2.2 is required to convert struct as json string which is the only solution possible as of today.
    //Types like struct can have any data types and hence its difficult to predict how many nested may come and duplicate column needs to merge ,
    // naming convention fix and hence cannot convert to structured column . So removing columns with datatypes which cannot be handled without knowing the schema of the column before in hand.
    for (colName <- vootTransformDF.columns) {
      val colDataType = vootTransformDF.schema(colName).dataType
      if (
          (colDataType == LongType) || (colDataType == DoubleType) || (colDataType == BooleanType) ||
          (colDataType == ByteType) || (colDataType == ShortType) || (colDataType == IntegerType) ||
          (colDataType == FloatType)|| (colDataType == TimestampType) || (colDataType == DecimalType) ||
          (colDataType == DateType)
      ) {
        vootTransformDF = vootTransformDF.withColumn(colName, col(colName).cast("string"))
      }else{
        if( (colDataType != StringType)){
          println("Dropped ", colName," ",colDataType )
          vootTransformDF = vootTransformDF.drop(colName)
        }
      }
    }



    // Merge  Duplicate Columns . Remove duplicate Columns after merging.
    val columnsNames = vootTransformDF.columns.map(a => a.replace(" ", "remove").replaceAll("[^A-Za-z0-9 _]", "").replace("remove", "_").toLowerCase)
    val columnNamesAsList = vootTransformDF.columns.toList
    val duplicateColumnNames = columnsNames.diff(columnsNames.distinct) // get duplicate=actual-distinct
    var duplicateNameslist = List[String]()
    for (dupcolname <- duplicateColumnNames) {
      duplicateNameslist = columnNamesAsList.filter(coltobeadded => coltobeadded.replace(" ", "remove").replaceAll("[^A-Za-z0-9 _]", "").replace("remove", "_").toLowerCase == dupcolname)
      vootTransformDF = vootTransformDF.withColumn(duplicateNameslist(0) + "_tmp", concat_ws(",", duplicateNameslist.map(colName => col(colName)): _*)) // concat multiple columns to Single column.
      duplicateNameslist.map(colName => vootTransformDF = vootTransformDF.drop(colName)) //removes columns after merge
      vootTransformDF = vootTransformDF.withColumnRenamed(duplicateNameslist(0) + "_tmp", duplicateNameslist(0)) // rename temporary column name to actual.
      }

    //Rename columns adhereing to SQL naming conventions . Remove characters other than alpha numeric except underscore_ and space.
    for (renameCol <- vootTransformDF.columns) {
      val renamedCol = renameCol.replace(" ", "remove").replaceAll("[^A-Za-z0-9 _]", "").replace("remove", "_").toLowerCase //regex [^A-Za-z0-9 _] for non-alphanumeric
      vootTransformDF = vootTransformDF.withColumnRenamed(renameCol, renamedCol)
    }

    //remove column name starting with underscore.As we allowed underscore in column name in previous step there is chance than the column name comes starting with multiple undersccore
    //eg: __mps , _colname_ab. So remove undersccore in the starting of the column .
    for (renameCol <- vootTransformDF.columns) {
      // println("renameCol.head ",renameCol ,renameCol.head)
      if (renameCol.head=='_') {
        println("o_col ",renameCol)
        val renamedCol = "[^A-Za-z0-9 ]+".r.replaceFirstIn(renameCol,"")
        println("r_col ",renamedCol)
        vootTransformDF = vootTransformDF.withColumnRenamed(renameCol, renamedCol)
      }
    }
    //remove scala/spark sepecific reserved characters which are not allowed inside a string literal.These characters have special meaning to spark/scala.
    //remove . dot from column names. As dot is considered as operator by dataframe and will throw error while retreving  it..
    //remove \ backslash . Spark scala will treat \ inside a string as starting an escape sequence. eg: \" allows " inside a string,\t allows tabs in a string.
    // remove ` backtick.In scala will treat ` to allow almost any  characters between `. method name can be given as  eg: def `Given I login as user`(username:String) = { }
    val reservedCharacters = List(".","\\","`","'")
    for (renameCol <- vootTransformDF.columns) {
      if (reservedCharacters.exists(renameCol.contains(_))) {
        val renamedCol = renameCol.replace(".", " ").replace("\\", " ").replace("`", " ")
        vootTransformDF = vootTransformDF.withColumnRenamed(renameCol, renamedCol)
      }
    }

    //replace  empty string to null.
    vootTransformDF = setEmptyToNull(vootTransformDF)

    println(" write to parquet started  !!", Calendar.getInstance().getTime())
    println("writing parque @ outputfilepath", outputfilepath)

    //Load Layer
    vootTransformDF.write.partitionBy("date_stamp").format("parquet").mode("append").option("spark.sql.parquet.compression.codec", "gzip").save(outputfilepath) //Write to Parquet Files.
    val count = vootTransformDF.count.toInt
    val end_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val min = sqlcontext.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString

    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")
    sqlcontext.sql(s"""insert into default.DataTableloadHistory partition(date_Stamp) select CURRENT_DATE ,CURRENT_DATE ,'ETL' ,'$loadtype','APP', '$count','$st_time','$end_time','$min','yes','$jobtype','$dateset' as date_stamp from DataTableloadHistory limit 1""")
    println(" Inserted to table  default.DataTableloadHistory !!", count)
    println(" write to parquet completed !!", Calendar.getInstance().getTime())
    println("processVootAppData ****************END**************", Calendar.getInstance().getTime())

    } catch {
      case e: Exception => {
        //exceptions are logged in individual spark job logs. These can be tracked in job history /yarnui
        println("VOOT_MOBILE_SPARK_ETL ERROR !!!   :")
        println(e)

      }
        throw new Exception("VOOT_MOBILE_SPARK_ETL ERROR !!!   :"+e)
        //throwing the caught expception again  to make spark job status to failed .Else the job status will show succeeded
      // inspite of exception
    }

  }


  def main(args: Array[String]) {

    try {
      val inputJsonFile = args(0)
      val outputParquetFilesDirectory = args(1) //output directory path
      val jobtype = args(2)
      val dateset = args(3)
      val loadtype = args(4)
      println("output file path :"+ inputJsonFile)
      println("output file path : "+outputParquetFilesDirectory)
      println("jobtype "+jobtype)
      println("date set "+dateset)
      println("loadtype set "+loadtype)
      val datepattern = "(\\d{4}-\\d{2}-\\d{2})".r
      val getDateFromFileName = datepattern.findFirstIn(inputJsonFile).get
      processVootAppData(inputJsonFile, outputParquetFilesDirectory,jobtype,dateset,loadtype)
    } catch {
      case e: Exception => {
        println("VOOT_MOBILE_SPARK_ETL ERROR !!!   :")
        println(e)
      }

        throw new Exception("VOOT_MOBILE_SPARK_ETL ERROR !!!   :"+e)


    }

  }

}



