import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io._
import org.apache.spark.sql.functions._

object Task1 {
  def main(args:Array[String]): Unit= {
    val spark = SparkSession
      .builder()
      .appName("Task1")
      .master("local[2]")
      .config("spark.some.config.option","some-value")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", "true")
      .csv(args(0))
    var df_sel: DataFrame  = df.select("Country", "Salary","SalaryType").withColumn("Country", regexp_replace(col("Country"),"\\,",""))
    var df_filter: DataFrame = df_sel.filter((!(col("Salary").equalTo("NA")) && !(col("Salary").equalTo("0")) ))
    var ans: DataFrame = df_filter.groupBy("Country").count().orderBy(asc("Country"))
    var total = ans.agg(sum("count")).first().get(0)

    val out = new PrintWriter(args(1))
    out.write("Total"+","+total+"\n")
    for (row <- ans.collect()) {
      val line = row.get(0) + ","+row.get(1)+"\n"
      out.write(line)
    }
    out.close()
 }
}
