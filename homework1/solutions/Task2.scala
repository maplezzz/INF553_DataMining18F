import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io._

import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD._


object Task2 {
  //default method used in task1
  def main(args:Array[String]): Unit= {
    val spark = SparkSession
      .builder()
      .appName("task2")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", "true")
      .csv(args(0))
      //.csv("data/survey_results_public.csv")
    var df_sel: DataFrame  = df.select("Country", "Salary","SalaryType")
    var df_filter: DataFrame = df_sel.filter((!(col("Salary").equalTo("NA")) && !(col("Salary").equalTo("0")) ))
    var x =  df_filter.rdd.getNumPartitions
    var y = df_filter.rdd.mapPartitions(iter => Array(iter.size).iterator, true)
    var temp = df_filter.select("Country").rdd.map(x => (x,1))
    val t1 = System.nanoTime
    temp.reduceByKey((a,b) => a + b).collect()
    val dur_std = (System.nanoTime - t1) / 1e6d




    val range = df_filter.select("Country").repartitionByRange(2,col("Country")).orderBy(asc("Country")).rdd.map(x => (x,1))
    var x_p =  range.getNumPartitions
    var y_p = range.mapPartitions(iter => Array(iter.size).iterator, true)
    val t2 = System.nanoTime
    range.reduceByKey((a,b) => a + b).collect()
    val dur_p = (System.nanoTime - t2) / 1e6d

    val out = new PrintWriter(args(1))
    //val out = new PrintWriter("data/Task2.csv")
    out.write("standard"+","+ y.collect().mkString(",")+","+dur_std+"\n")
    out.write("partition" + "," + y_p.collect().mkString(",") + "," + dur_p)
    out.close()

  }
}
