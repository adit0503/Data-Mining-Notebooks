import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{BufferedWriter, File, FileWriter}
import scala.util.parsing.json.JSONObject

object Task2 {

  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Aditya_Chavan_Clustering").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)

    val input_data = sc.textFile(args(0))
    //val input = sctxt.textFile("/Users/mahima/IdeaProjects/DMAssignment4/src/main/scala/yelp_reviews_clustering_small.txt")
    val seed = 42
    val Al = args(1).toString
    //val AL = "B"
    val Nc =args(2).toInt
    //val num_clusters = 8
    val Ni = args(3).toInt
    //val num_iterations = 20

    val docu: RDD[Seq[String]] = input_data.map(_.split(" ").toSeq)
    var output_file =" "
    if(Al == "K") {output_file = "Aditya_Chavan_Clustering_small_K_8_20.json"}
    if(Al == "B") {output_file = "Aditya_Chavan_Clustering_small_B_8_20.json"}
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(docu)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    tfidf.cache()
    val bfwr = new BufferedWriter(new FileWriter(new File(output_file)))
    bfwr.write("{")
    bfwr.flush()
    if (Al == "K")
    {
      bfwr.append("\"algorithm\" : \"K-Means\",")
      val cl = KMeans.train(tfidf, Nc, Ni, "random", 42)
      val clc = cl.clusterCenters
      val pd = cl.predict(tfidf)
      val fea = tfidf.zip(docu).zip(pd).collectAsMap()
      val ov = fea.map(x => (((x._1._2, x._1._1), Vectors.sqdist(x._1._1, cl.clusterCenters(x._2))), x._2))
      val wsse = cl.computeCost(tfidf)
      bfwr.append("\"WSSE\" : \"" + wsse + "\",")
      var wsse2 = 0.0
      bfwr.append("\"clusters\" : [ ")
      for (i <- 0 to Nc - 1)
      {
        val cl1 = ov.filter(a => a._2 == i).toSeq
        var cl1e = 0.0
        var w1 = List[String]()
        cl1.foreach(y => {
          cl1e = cl1e + y._1._2
          w1 = w1 ::: y._1._1._1.toList
        })
        wsse2 += cl1e
        val t10 = w1.groupBy(identity).toList.sortBy(_._2.size).reverse.take(10).map(_._1).toArray
        bfwr.append("{ \"id\" : " + (i+1) + ",")
        bfwr.append("\"size\" : " + cl1.length + ",")
        bfwr.append("\"error\" : " + cl1e + ",")
        bfwr.append("\"terms\" : [ ")
        val t10s = t10.mkString("\",\"")
        bfwr.append("\"" + t10s + "\"")
        bfwr.append(" ] }")
        if (i < Nc - 1) {
          bfwr.append(",")
        }
      }
      bfwr.write("]")
      bfwr.write("}")
      bfwr.close()
    }
    else if (Al == "B")
    {
      bfwr.append("\"algorithm\" : \"Bisecting K-Means\",")
      val bkm = new BisectingKMeans().setK(Nc).setMaxIterations(Ni).setSeed(42)
      val clu = bkm.run(tfidf)
      val wsse = clu.computeCost(tfidf)
      val clcent = clu.clusterCenters
      val pr = clu.predict(tfidf)
      val fea = tfidf.zip(docu).zip(pr).collectAsMap()
      val ovec = fea.map(m => (((m._1._2, m._1._1), Vectors.sqdist(m._1._1, clu.clusterCenters(m._2))), m._2))
      bfwr.append("\"WSSE\" : \"" + wsse + "\",")
      var wsse2 = 0.0
      bfwr.append("\"clusters\" : [ ")
      for (i <- 0 to Nc - 1) {
        val cl2 = ovec.filter(x => x._2 == i).toSeq
        val t10 = ovec.filter(m => m._2 == i).map(q => q._1._1._2)
        var cl2e = 0.0
        var w1 = List[String]()
        cl2.foreach(row => {
          cl2e = cl2e + row._1._2
          w1 = w1 ::: row._1._1._1.toList
        })
        wsse2 += cl2e
        val t10w = w1.groupBy(identity).toList.sortBy(_._2.size).reverse.take(10).map(_._1).toArray
        bfwr.append("{ \"id\" : " + (i+1) + ",")
        bfwr.append("\"size\" : " + cl2.length + ",")
        bfwr.append("\"error\" : " + cl2e + ",")
        bfwr.append("\"terms\" : [ ")
        val t10s = t10w.mkString("\",\"")
        bfwr.append("\"" + t10s + "\"")
        bfwr.append(" ] }")
        if (i < Nc - 1)
        {
          bfwr.append(",")
        }
      }
      bfwr.write("]")
      bfwr.write("}")
      bfwr.close()
    }
  }
}
