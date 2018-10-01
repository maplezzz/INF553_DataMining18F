import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Task3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Task3")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", "true")
      .csv(args(0))
    var df_sel: DataFrame = df.select("Country", "Salary", "SalaryType").withColumn("Salary", regexp_replace(col("Salary"),"\\,","")).withColumn("Country", regexp_replace(col("Country"),"\\,",""))
    var df_filter: DataFrame = df_sel.filter((!(col("Salary").equalTo("NA")) && !(col("Salary").equalTo("0")) ))
    //convert col("SalaryType") === 0 to annual
    var df_update: DataFrame = df_filter.withColumn("SalaryType", when(col("SalaryType").equalTo("NA"), "Yearly").otherwise(col("SalaryType")))
    var df_allYear = df_update.withColumn("Salary", when(col("SalaryType").equalTo("Monthly"), (col("Salary")*12).cast(IntegerType))
                      .when(col("SalaryType").equalTo("Weekly"), (col("Salary")*52).cast(IntegerType)).otherwise(col("Salary")))
    var df_final = df_allYear.select("Country", "Salary").withColumn("Salary", col("Salary").cast(IntegerType)).orderBy(asc("Country"))

    var df_group = df_final.groupBy("Country").agg(count("Salary") as "count",min("Salary") as "min", max("Salary") as "max", mean("Salary") as "mean")
    var df_output = df_group.withColumn("mean", round(col("mean"),2))

    val out = new PrintWriter(args(1))
    for (row <- df_output.collect()) {
      val line = row.get(0) + ","+row.get(1)+ ","+row.get(2)+ ","+row.get(3)+","+row.get(4)+"\n"
      out.write(line)
    }
    out.close()

  }

}