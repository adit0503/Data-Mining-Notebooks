import java.io._
import util.control.Breaks._
import org.apache.spark.{SparkConf, SparkContext}

object Aditya_Chavan_SON {
  def main(args: Array[String]) : Unit = {
    val start_time = System.nanoTime()
    val conf = new SparkConf().setAppName("Aditya_Chavan_Task").setMaster("local")
    val sc = new SparkContext(conf)

    val input_file = args(0)
    //val input_file = "C:/Users/Aditya/IdeaProjects/DM_Assignment3/src/main/scala/yelp_reviews_small.txt"
    val support = args(1).toInt
    //val support = 1000
    val output_file = args(2)
    //val output_file = new File("Aditya_Chavan_SON_yelp_reviews.txt")

    val raw_data = sc.textFile(input_file)
    val data = raw_data.map(x => x.split(","))
    val data_1 = data.map(x => (x(0), x(1)))
    val data_2 =  data_1.groupByKey()
    val baskets =  data_2.map(x => x._2.toSet)
    val threshhold_value = support/raw_data.getNumPartitions
    val first_1 = baskets.mapPartitions(x => {Apriori(x.toList, threshhold_value)})
    val first_2 = first_1.map(x => (x,1))
    val first_3 = first_2.reduceByKey((x1,x2) => 1)
    val first_4 = first_3.keys.collect()
    val first_map = sc.broadcast(first_4)
    val second_map = baskets.mapPartitions(x => {
      var out_list = List[(Set[String],Int)]()
      for (i <- x.toList){ for (j <- first_map.value){ if (j.forall(i.contains)){ out_list = Tuple2(j,1) :: out_list}}}
      out_list.iterator
    })
    val final_1 = second_map.reduceByKey(_+_)
    val final_2 = final_1.filter(x => x._2 >= support)
    val final_3 = final_2.keys
    val final_map = final_3.map(x => (x.size,x)).collect()
    val frequent_1 = final_map.filter(x => x._1 == 1)
    val frequent_items = frequent_1.map(x => x._2)
    val sorted_items = Sort_items(frequent_items)
    val wr = new PrintWriter(output_file)
    for (x <- sorted_items){
      if (x == sorted_items.last){ wr.write(x.mkString( "(" , ", " , ")\n\n" ))}
      else { wr.write(x.mkString( "(" , ", " , "), " ))}
    }
    var str = ""
    val max_value = final_map.maxBy(_._1)._1
    for (p <- 2.to(max_value)){
      val temp_1 = final_map.filter(x => x._1 == p)
      val temp_2 = temp_1.map(x => x._2)
      val x = Sort_items(temp_2)
      for (q <- x) { str += "("; val s = q.toList.sorted; for (r <- s){ if (r == s.last){ str += "" + r + "), " } else { str += "" + r +"," } } }
      str = str.dropRight(2) + "\n\n"
    }
    wr.write(str)
    wr.close()
    val end_time = System.nanoTime()
    val time = (end_time-start_time)/1000000000
    println("Time: " +  time  + " sec.")
  }
  def Apriori(basket: List[Set[String]], threshold:Int) : Iterator[Set[String]] = {
    val single_flat = basket.flatten
    val single_gb = single_flat.groupBy(identity)
    val single_s = single_gb.mapValues(x => x.size)
    val single_f= single_s.filter(x => x._2 >= threshold)
    val single_items = single_f.keys.toSet
    var map_singleitems = Set.empty[Set[String]]
    for (i <- single_items){map_singleitems += Set(i)}
    var result = Set.empty[String]
    var map_size = map_singleitems.size
    var frequent_items = single_items
    var size = 2
    while (frequent_items.size >= size) {
      val candidate_items = frequent_items.subsets(size)
      for (temp <- candidate_items){ breakable{
          var count = 0
          for(item <-basket){ if(temp.subsetOf(item)){ count += 1; if(count >= threshold){ result = result ++ temp; map_singleitems += temp; break}}}}
      }
      if(map_singleitems.size > map_size){frequent_items = result; result = Set.empty[String]; size += 1; map_size = map_singleitems.size}
      else{ size = 1 + frequent_items.size}
    }
    map_singleitems.iterator
  }
  def Sort_items[X : Ordering](c: Seq[Iterable[X]]) : Seq[Iterable[X]] = c.sorted
}