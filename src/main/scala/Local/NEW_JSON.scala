package Local


object NEW_JSON {
  import org.apache.spark.sql.SQLImplicits
  import org.apache.spark.sql.{SQLContext, SparkSession}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions.explode


  import org.apache.spark.sql.functions.col



  val spark = SparkSession.builder().master("local[*]").appName("Spark_Local").getOrCreate()
  val sqlcontext: SQLContext = spark.sqlContext
  def main(args: Array[String]) {
    //val dfs = sqlcontext.read.json("C:/Users/indian/Desktop/json/example_1.json")
    val dfs = sqlcontext.read.json("C:/Users/indian/Desktop/json/new.json")
    //val flattened = dfs.select($"name", explode($"schools").as("schools_flat"))
    dfs.printSchema()
    dfs.show()
   // flattened.show()
  }
}
