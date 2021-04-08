import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.{File, PrintWriter}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object ModelBasedCF {
  def main(args: Array[String]) : Unit ={
    val start_time = System.nanoTime()
    val conf = new SparkConf().setAppName("Aditya_Chavan_Task1").setMaster("local")
    val sc = new SparkContext(conf)
    val train_dataset = args(0)
    val test_dataset = args(1)
    //val train_dataset = "C:/Users/Aditya/IdeaProjects/DM_Assignment2/src/main/scala/train_review.csv"
    //val test_dataset = "C:/Users/Aditya/IdeaProjects/DM_Assignment2/src/main/scala/test_review.csv"
    val train_dataset_file = sc.textFile(train_dataset)
    val test_dataset_file = sc.textFile(test_dataset)
    val train_header = train_dataset_file.first()
    val test_header = test_dataset_file.first()
    val train_rdd_string = train_dataset_file.filter(x=>x!=train_header).map(line => line.split(",")).map{x => ((x(0),x(1)),x(2).toDouble)}
    val test_rdd_string = test_dataset_file.filter(x=>x!=test_header).map(line => line.split(",")).map{x => ((x(0),x(1)),x(2).toDouble)}
    val result = (train_rdd_string).++(test_rdd_string)
    var i: Int=1
    val map_userid = scala.collection.mutable.Map[String,Int]()
    val map_userid_ulta = scala.collection.mutable.Map[Int,String]()
    result.collect().foreach{row => if(map_userid.contains(row._1._1)){} else {map_userid += row._1._1 -> i; map_userid_ulta += i -> row._1._1; i += 1}}
    var j: Int=1
    val map_businessid = scala.collection.mutable.Map[String,Int]()
    val map_businessid_ulta = scala.collection.mutable.Map[Int,String]()
    result.collect().foreach{row =>if(map_businessid.contains(row._1._2)){}else { map_businessid += row._1._2 -> j; map_businessid_ulta += j -> row._1._2; j += 1}}
    val train_rdd = test_rdd_string.map(x => ((map_userid(x._1._1), map_businessid(x._1._2)), x._2))
    val test_rdd = test_rdd_string.map(x => ((map_userid(x._1._1), map_businessid(x._1._2)), x._2))
    val testing = test_rdd.map(x => (x._1._1,x._1._2))
    val training = train_rdd.map(x => (x._1._1, x._1._2, x._2) match {case (user, business, star) => Rating(user.toInt, business.toInt, star.toDouble)})
    val rank = 2; val numIterations = 18; val alpha = 0.3; val block = 1; val seed = 8
    val model = ALS.train(training, rank, numIterations,alpha,block,seed)
    val predictions = model.predict(testing).map{case Rating(user, business, star) => ((user, business),star)}
    val predictions_map = predictions.collect().toMap
    val not_predicted = test_rdd.filter(x => !predictions_map.keySet.contains(x._1))
    val not_predicted_star = not_predicted.join(test_rdd).map{case((user,business),(predicted,stars)) => ((user,business), stars)}
    val predictions_total = predictions.++(not_predicted_star)
    val predictions_normalized = predictions_total.map{case((user, business), predicted_star) => if (predicted_star <= 0) ((user, business), 0.0) else if (predicted_star >= 5) ((user, business), 5.0) else ((user, business), predicted_star)}
    val output_file_path = "Aditya_Chavan_ModelBasedCF.txt"
    val output_file = new PrintWriter(new File(output_file_path))
    output_file.println("user_id,business_id,stars")
    val predictions_normalized_string = predictions_normalized.map(x => ((map_userid_ulta(x._1._1),map_businessid_ulta(x._1._2)),x._2))
    val row = predictions_normalized_string.sortByKey().map(x => x._1._1 + "," + x._1._2 + "," + x._2).toLocalIterator
    while(row.hasNext)
      output_file.println(row.next())
    output_file.close()
    val true_and_predicted = test_rdd.join(predictions_normalized)
    val MSE = true_and_predicted.map{case((user,business),(true_star,predicted_star))=>val error=true_star-predicted_star
      error * error
    }.mean()
    val c0=true_and_predicted.map{case ((user,business),(r1,r2))=>((user,business),Math.abs(r1-r2))}.filter(x=>x._2 >=0 && x._2<1).count()
    val c1=true_and_predicted.map{case ((user,business),(r1,r2))=>((user,business),Math.abs(r1-r2))}.filter(x=>x._2 >=1 && x._2<2).count()
    val c2=true_and_predicted.map{case ((user,business),(r1,r2))=>((user,business),Math.abs(r1-r2))}.filter(x=>x._2 >=2 && x._2<3).count()
    val c3=true_and_predicted.map{case ((user,business),(r1,r2))=>((user,business),Math.abs(r1-r2))}.filter(x=>x._2 >=3 && x._2<4).count()
    val c4=true_and_predicted.map{case ((user,business),(r1,r2))=>((user,business),Math.abs(r1-r2))}.filter(x=>x._2 >=4).count()
    println(">=0 and <1: "+c0)
    println(">=1 and <2: "+c1)
    println(">=2 and <3: "+c2)
    println(">=3 and <4: "+c3)
    println(">=4: "+c4)
    println("RMSE: "+Math.sqrt(MSE))
    val end_time = System.nanoTime()
    val time = (end_time-start_time)/1000000000
    println("Time: " +  time  + " sec.")
  }
}