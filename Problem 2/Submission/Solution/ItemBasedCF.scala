import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math._
import java.io._

object ItemBasedCF {
  def main(args: Array[String]):Unit ={
    val start_time = System.nanoTime()
    val conf = new SparkConf().setAppName("Sample Application").setMaster("local")
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
    val test_rdd_avg = test_rdd_string.map(x => ((map_userid(x._1._1), map_businessid(x._1._2)), 2.467648379465))
    val avg_dataset = train_rdd.++(test_rdd_avg)
    val business_length =  avg_dataset.map{case((user, business),rate) => (business, rate)}.groupByKey().map(elem => (elem._1, elem._2.size))
    val business_sum =  avg_dataset.map{case((user, business),rate) => (business, rate)}.reduceByKey(_ + _)
    val business_avg = business_sum.join(business_length).map{case(user, (sum, len)) => (user.toInt, sum / len)}
    val avg_map_business = business_avg.collectAsMap()
    test_rdd.collect().foreach{ x => if (avg_map_business.contains(x._1._2)){} else{avg_map_business.updated(x._1._2, 3.154376493185)}}
    val predictions = test_rdd.map(x => ((x._1._1,x._1._2),avg_map_business(x._1._2)))
    val predictions_normalized = predictions.map{case((user, business), predicted_star) => if (predicted_star <= 0) ((user, business), 0.0) else if (predicted_star >= 5) ((user, business), 5.0) else ((user, business), predicted_star)}
    val itemMap = avg_dataset.map{ case ((userid, moveid), stars) => (moveid.toInt,(userid.toInt,stars.toDouble))}
    val group_item = itemMap.groupByKey()
    val avg_item_rating = group_item.flatMap(f=>create_Average(f._1 ,f._2 ))
    val join_avg = group_item.join(avg_item_rating)
    val normalized_ratings = join_avg.flatMap(f=>find_Difference(f._1 ,f._2 ))
    val normal_rat = normalized_ratings.collect.toMap
    val item_rat = avg_dataset.collect.toMap
    val avg_rat = avg_item_rating.collect.toMap
    val userMap = avg_dataset.groupBy(f=>f._1 ._1 ).collect.toMap
    val finaldata = avg_dataset.mapPartitions(f=> call_pearson_correlation(f,normal_rat,item_rat,userMap,avg_rat).iterator)
    val finaldata_filtered = finaldata.filter { case((user, product), r) => !java.lang.Double.isNaN(r)}
    val maprightpart = finaldata_filtered.collect()
    val output_file_path = "Aditya_Chavan_ItemBasedCF.txt"
    val output_file = new PrintWriter(new File(output_file_path))
    output_file.println("user_id,business_id,stars")
    val predictions_normalized_string = predictions_normalized.map(x => ((map_userid_ulta(x._1._1),map_businessid_ulta(x._1._2)),x._2))
    val row = predictions_normalized_string.sortByKey().map(x => x._1._1 + "," + x._1._2 + "," + x._2).toLocalIterator
    while(row.hasNext)
      output_file.println(row.next())
    output_file.close()
    val true_and_predicted = test_rdd.join(predictions_normalized)
    val MSE = true_and_predicted.map{ case ((user, business),(true_star, predicted_star)) => val error = true_star - predicted_star
      error * error
    }.mean()
    val c0 = true_and_predicted.map{case ((user, business),(r1, r2)) =>((user, business), Math.abs(r1 - r2))}.filter(x => x._2 >= 0 && x._2 < 1).count()
    val c1 = true_and_predicted.map{case ((user, business),(r1, r2)) =>((user, business), Math.abs(r1 - r2))}.filter(x => x._2 >= 1 && x._2 < 1.8).count()
    val c2 = true_and_predicted.map{case ((user, business),(r1, r2)) =>((user, business), Math.abs(r1 - r2))}.filter(x => x._2 >= 1.8 && x._2 < 2.2).count()
    val c3 = true_and_predicted.map{case ((user, business),(r1, r2)) =>((user, business), Math.abs(r1 - r2))}.filter(x => x._2 >= 2.2 && x._2 < 2.5).count()
    val c4 = true_and_predicted.map{case ((user, business),(r1, r2)) =>((user, business), Math.abs(r1 - r2))}.filter(x => x._2 >= 2.5).count()
    println(">=0 and <1: " + c0)
    println(">=1 and <2: " + c1)
    println(">=2 and <3: " + c2)
    println(">=3 and <4: " + c3)
    println(">=4: " + c4)
    println("RMSE: " + Math.sqrt(MSE))
    val end_time = System.nanoTime()
    val time = (end_time - start_time) / 1000000000
    println("Time: " +  time  + " sec.")
  }
  def create_Average(businessid:Int, userid_stars: Iterable[(Int, Double)]) : Map[Int,Double]= {
    var tempmap =Map[Int,Double]();val count = userid_stars.size;var sum:Double = 0;for (i <- userid_stars ){sum += i._2};tempmap = Map(businessid->(sum/count))
    return tempmap
  }
  def find_Difference(businessid:Int, row:(Iterable[(Int, Double)], Double)): Map[Int,scala.collection.mutable.Map[Int,Double]]= {
    var tempmap =scala.collection.immutable.Map[Int,scala.collection.mutable.Map[Int,Double]]();var temp = scala.collection.mutable.Map[Int,Double]();for (i<-row._1 ){temp +=  i._1 -> (i._2 -row._2)};tempmap += (businessid-> temp)
    return tempmap
  }
  def find_pearson_correlation(businessid1:Int, businessid2:Int, normalizedmap:Map[Int,scala.collection.mutable.Map[Int,Double]]) : Double={
    val i = normalizedmap(businessid1);val j = normalizedmap(businessid2)
    var sum:Double =0
    for (k <- i.keys){if (j.contains(k)){sum = sum + j(k) * i(k)}}
    var r1:Double =0; var r2:Double =0
    for (k <- i.keys){r1 = r1 + i(k)*i(k)}
    for (k <- j.keys){r2 = r2 + j(k)*j(k)}
    val denominator = (sqrt(r1)*sqrt(r2))
    if (denominator!=0){val pearson_correlation = sum / denominator; val pearsons = pearson_correlation * Math.pow(Math.abs(pearson_correlation),(1.5))
      return pearsons
    }
    else
      return 0.001
  }
  def call_pearson_correlation(item:Iterator[((Int, Int), Double)],normalizedmap:Map[Int,scala.collection.mutable.Map[Int,Double]],tempomap: Map[(Int, Int),Double],usermap:Map[Int,Iterable[((Int, Int), Double)]], avgmap:Map[Int,Double]) :  scala.collection.mutable.Map[(Int,Int),Double]= {
    val tempmap = scala.collection.mutable.Map[(Int,Int),Double]()
    for (i<- item){
      var tempMap2 = scala.collection.mutable.Map[Int,Double]();var p_datamap =  Map[(Int,Int),Double]();val MovieId = i._1 ._2;val user = i._1 ._1
      if (normalizedmap.contains(MovieId)) {
        var flag = 1;for (j <- normalizedmap.keys){
          if (j != MovieId){
            if (normalizedmap(j).contains(user)){
              flag = 0;p_datamap += (MovieId,j)-> find_pearson_correlation(MovieId,j,normalizedmap)}}}
        if (flag ==1){
          tempmap += (user,MovieId) ->  avgmap(MovieId)}
        else{
          val nearest = p_datamap.toSeq.sortBy(_._2).reverse.take(20); var numerator:Double =0; var denominator1:Double = 0; var denominator2:Double = 0
          for (n<-nearest){val u = tempomap(user,n._1 ._2);numerator = numerator + (u*n._2 );denominator1 = denominator1 + Math.abs(n._2)}
          if ((numerator/denominator1)>5){tempmap += (user,MovieId)-> (avgmap(MovieId))}
          else if((numerator/denominator1)<0){tempmap += (user,MovieId)-> (avgmap(MovieId))}
          else{tempmap += (user,MovieId)->(numerator/denominator1)}}}
      else{
        if (usermap.contains(user)){
          val vect = usermap(user);var sum:Double=0;val c:Double = vect.size
          for (i <- vect){sum=sum + i._2};tempmap += (user,MovieId) -> sum/c}
        else{
          tempmap += (user,MovieId) -> 2.5}}}
    return tempmap
  }
}
