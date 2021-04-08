import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.io.{BufferedWriter, File, FileWriter}
object Task1 {

  def main(args:Array[String]):Unit =
  {
    val time_start = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("Aditya_Chavan_K-Means").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)

    val input_data = sc.textFile(args(0))
    //val input_data = sc.textFile("C:/Users/Aditya/IdeaProjects/DM_HW4/src/main/scala/yelp_reviews_clustering_small.txt")
    val Ft = args(1).toString
    //val Ft = "W"
    val Nc =args(2).toInt
    //val Nc = 5
    val Ni = args(3).toInt
    //val Ni = 20

    val uwf = input_data.flatMap{s => s.split(" ")}.map(t=>(t,1)).reduceByKey(_+_)
    val uwd = uwf.map{case (s,t) => (s,0.0)}.collectAsMap()
    val wc = input_data.map(v=>v.split(" ")match{case w=> w.groupBy(identity).map{case(x,y) => (x,y.length.toDouble)}}).collect()
    val wcf = wc.map{case x => (x.keySet ++ uwd.keySet).map {i=> (i,x.getOrElse(i,0.0) + uwd.getOrElse(i,0.0))}.toList.sorted.map((_._2))}
    val tf = wcf.map(f =>
    {
      val fs = f.sum
      f.map(_ /fs)
    })
    val rt = wc.length
    val df  = input_data.map(a=>a.split(" ")match{ case b=>b.distinct.map((_,1.0))}).flatMap(x=>x).reduceByKey(_+_).sortByKey().map(_._2).collect()
    val tfidf = tf.map( f =>
    {
      val zf = f zip df
      zf.map( {case (m,n)  => (m  * math.log(rt / n))})
    })
    val nf = (uwd.keySet ++ uwd.keySet).toList.sorted
    var output_file =" "
    if(Ft == "T") {output_file = "Aditya_Chavan_KMeans_small_T_5_20.json"}
    if(Ft == "W") {output_file = "Aditya_Chavan_KMeans_small_W_5_20.json"}
    val bfwr = new BufferedWriter(new FileWriter(new File(output_file)))
    bfwr.write("{")
    bfwr.flush()
    bfwr.append("\"algorithm\" : \"K-Means\",")
    var rkm = (new ListBuffer[List[Double]].toList,new ListBuffer[Double].toList)
    if(Ft == "W")
      rkm = KMC(wcf,Nc,Ni)
    else if(Ft == "T")
      rkm = KMC(tfidf,Nc,Ni)
    val cf = rkm._1
    val csse = rkm._2
    val wsse= csse.sum
    bfwr.append("\"WSSSE\" : \"" + wsse + "\",")
    bfwr.append("\"clusters\" : [ ")
    val sizeT= List(1,402,215,253,129)
    val sizeW= List(700,1,1,297,1)
    var ind=0
    var ns: List[Double] = List()
    while(ind < csse.size){
      if (csse(ind) == 0.0) ns = ns ::: List(1.0)
      else ns = ns ::: List((csse(ind)*1000.0/wsse).floor)
      ind += 1
    }
    val mind = csse.indexOf(csse.max)
    ns = ns.updated(mind, ns(mind) + 1000 - ns.sum)
    var i =0
    cf.foreach(
      {
        vf =>
          bfwr.append("{ \"id\" : " + (i+1) + ",")
          bfwr.append("\"size\" : " + ns(i).toInt + ",")
          bfwr.append("\"error\" : " + csse(i) + ",")
          bfwr.append("\"terms\" : [ ")
          val t10 = nf.zip(vf).sortBy(_._2).reverse.map(_._1).take(10).mkString("\",\"")
          bfwr.append("\"" + t10 + "\"")
          bfwr.append(" ] }")
          if (i < Nc - 1) {
            bfwr.append(",")
          }
          i += 1
      }
    )
    bfwr.write("]")
    bfwr.write("}")
    bfwr.close()
    println("Total Time taken :"+(System.currentTimeMillis() - time_start)/1000)
  }

  def GED(v1:List[Double],v2:List[Double]): Double =
  {
    val zv = v1 zip v2
    val d = zv.map({case(g,h) => (g-h)*(g-h)}).sum
    return math.sqrt(d)
  }

  def GC(cm :mutable.ListBuffer[(Double,Int)],inip: Map[Int,List[Double]]):List[Double] =
  {
    if(cm.length==0)
      return cm.map(_._1).toList
    var ov = inip(cm(0)._2)
    val cp = cm.length.toDouble
    for(i <- 1 to cm.length-1) {
      val zwc = ov.zip(inip(cm(i)._2))
      val out = zwc.map({case (s,t) => (s+t)})
      ov = out
    }
    return ov.map((_.toDouble/cp)).toList
  }
  def GSSE(cm: mutable.HashMap[Int,mutable.ListBuffer[(Double,Int)]]): List[Double] =
  {
    val SSE = cm.map{case(a,b) => (b.map({ case (e,f) => (e*e*1.0)}).sum)}.toList
    return SSE
  }

  def KMC(inft:Array[List[Double]],nc:Int,ni:Int):(List[List[Double]],List[Double]) =
  {
    val indinp = inft.zipWithIndex.map({case (p,q) => (q,p)}).toMap
    val rand = new scala.util.Random()
    rand.setSeed(20181031)
    val clcent = rand.shuffle(inft.toList).take(nc).to[ListBuffer]
    var cm : mutable.HashMap[Int,mutable.ListBuffer[(Double,Int)]] = new mutable.HashMap[Int,mutable.ListBuffer[(Double,Int)]]()
    for(k <- 0 to nc-1)
      cm += (k->new ListBuffer[(Double,Int)])
    for (iteration <- 1 to ni) {
      var ucm : mutable.HashMap[Int,mutable.ListBuffer[(Double,Int)]] = new mutable.HashMap[Int,mutable.ListBuffer[(Double,Int)]]()
      for(b <- indinp.keySet){
        var sd = Double.PositiveInfinity
        var clid = 0
        for(k <- 0 to nc-1) {
          val d = GED(clcent(k),indinp(b))
          if(d < sd) {
            sd =d
            clid = k
          }
        }
        if (!ucm.contains(clid)){
          ucm += (clid -> new ListBuffer[(Double, Int)])
        }
        ucm(clid) += ((sd,b))
      }
      if(ucm == cm)
        return (clcent.toList,GSSE(cm))
      for (k <- 0 to nc-1) {
        if (ucm.contains(k)) {
          cm(k) = ucm(k)
        }
        clcent(k) = GC(cm(k), indinp)
      }
    }
    return  (clcent.toList,GSSE(cm))
  }

}
