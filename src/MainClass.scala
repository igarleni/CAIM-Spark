import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.SparkConf

object MainClass {
  def main(args:Array[String]): Unit = 
  {
    val conf = new SparkConf()
    conf.set("spark.cores.max", "20")
    conf.set("spark.executor.memory", "6g")
    conf.set("spark.kryoserializer.buffer.max", "512")
    conf.setAppName("IPDdiscretization")
    val sc = new SparkContext(conf)
    
    println("----------------App init----------------")
  }
}