import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Task3 {
  def main(args: Array[String]): Unit = {

    val inputfile_path = args(0)
    val outputfile_path = args(1)

    val spark = org
      .apache
      .spark
      .sql
      .SparkSession
      .builder
      .master("local[2]")
      .appName("DM_Assignment_Task_3")
      .getOrCreate

    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      //.load("C:/Users/Aditya/IdeaProjects/DM1/src/main/scala/survey_results_public.csv")
      .load(inputfile_path)
      .select("Country", "Salary", "SalaryType")

    val mydf = df
      .filter(x => (x(1) != "NA") && (x(1) != "0") )
    val myDF = mydf
      .withColumn("Salary", regexp_replace(mydf("Salary"), "\\,", ""))

    val salDF = myDF
      .withColumn("Salary", when(col("SalaryType").equalTo("Weekly"),(col("Salary")*52).cast("Double"))
        .otherwise(when(col("SalaryType").equalTo("Monthly"),(col("Salary")*12).cast("Double"))
          .otherwise((col("Salary")*1)).cast("Double")))

    val countDF = myDF
      .select("Country", "Salary")
      .groupBy("Country")
      .count()
      .sort(asc("Country"))

    val avgsalDF = salDF
      .select("Country","Salary")
      .groupBy("Country")
      .agg(bround(avg("Salary"),2) as "Average_Salary", min("Salary").cast(IntegerType) as "Min_Salary", max("Salary").cast(IntegerType) as "Max_Salary")
      .sort(asc("Country"))

    val cdf = countDF.as("CDF")
    val asdf = avgsalDF.as("ASDF")
    val joinDF = cdf
      .join(asdf, col("CDF.Country") === col("ASDF.Country"), "inner")
      .select("CDF.Country", "CDF.count", "ASDF.Min_Salary", "ASDF.Max_Salary", "ASDF.Average_Salary")

    joinDF
      .coalesce(1)
      .write.format("csv")
      //.save("C:/Users/Aditya/IdeaProjects/DM1/src/main/scala/results.csn")
      .save(outputfile_path)
  }
}