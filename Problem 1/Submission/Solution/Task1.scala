import org.apache.spark._
import java.io._

object Task1{
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
      .appName("DM_Assignment_Task_1")
      .getOrCreate

    val rdd = spark
      .read
      .format("csv")
      .option("header", "true") //reading the headers
      //.load("C:/Users/Aditya/IdeaProjects/DM1/src/main/scala/survey_results_public.csv")
      .load(inputfile_path)
      .select("Country", "Salary", "SalaryType")
      .rdd

    val myRDD = rdd
      .filter(x => (x(1) != "0") && (x(1) != "NA"))

    val newRDD = myRDD
      .map(x => ("\"" + x(0).toString + "\"",1))
      .reduceByKey(_+_)
      .sortByKey(ascending = true)
      .collect()

    //val pw = new PrintWriter(new File("C:/Users/Aditya/IdeaProjects/DM1/src/main/scala/result_1.csv"))
    val pw = new PrintWriter(new File(outputfile_path))
    pw.write("Total" + ", " + myRDD.count() + "\n")
    newRDD.foreach( x => pw.write( x._1 + ", " + x._2 + "\n"))
    pw.close()

  }
}