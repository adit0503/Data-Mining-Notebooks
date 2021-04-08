import org.apache.spark._
import java.io._

object Task2 {
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
      .appName("DM_Assignment_Task_2")
      .getOrCreate

    val rdd = spark
      .read
      .format("csv")
      .option("header", "true")
      //.load("C:/Users/Aditya/IdeaProjects/DM1/src/main/scala/survey_results_public.csv")
      .load(inputfile_path)
      .select("Country", "Salary", "SalaryType")
      .rdd

    val myRDD = rdd
      .filter(x => (x(1) != "0") && (x(1) != "NA"))

    val rddM1 = myRDD
      .map(x => (x(0).toString, 1))
    val rddM2 = myRDD
      .map(y => (y(0).toString, 1)).partitionBy(new HashPartitioner(2))

    val t1_1 = System.currentTimeMillis()
    val rddR1 =  rddM1
      .reduceByKey(_+_)
      .sortByKey(ascending = true)
    val d1 = (System.currentTimeMillis() - t1_1).toInt

    val t2_1 = System.currentTimeMillis()
    val rddR2 = rddM2
      .reduceByKey(_+_)
      .sortByKey(ascending = true)
    val d2 = (System.currentTimeMillis() - t2_1).toInt

    val sizeM1 = rddM1
      .mapPartitions(iter => Array(iter.size).iterator, true)
      .collect()
    val sizeM2 = rddM2
      .mapPartitions(iter => Array(iter.size).iterator, true)
      .collect()

    //val pw = new PrintWriter(new File( "C:/Users/Aditya/IdeaProjects/DM1/src/main/scala/result_2.csv" ))
    val pw = new PrintWriter(new File(outputfile_path))
    pw.write("Standard" + ", " + sizeM1(0) + ", " + sizeM1(1) + ", " + d1 + "\n")
    pw.write("Partition" + ", " + sizeM2(0) + ", " + sizeM2(1) + ", " + d2 + "\n")
    pw.close()
  }
}
