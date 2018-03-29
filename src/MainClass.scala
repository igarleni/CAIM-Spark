import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable.ArrayBuffer

object MainClass {
  var FILE_INPUT:String = null
  var FILE_CP_OUTPUT:String = null
  var FILE_DATA_OUTPUT:String = null
  var MEASURE_COLS:Int = 0
  var FIELD_DELIMITER:Char = ';'
  
  def main(args:Array[String]): Unit = 
  {
    
    val conf = new SparkConf()
    conf.set("spark.cores.max", "20")
    conf.set("spark.executor.memory", "6g")
    conf.set("spark.kryoserializer.buffer.max", "512")
    conf.setAppName("CAIMdiscretization")
    val sc = new SparkContext(conf)
    /*
    val conf = new SparkConf().setAppName("CAIMdiscretization").setMaster("local")
    val sc = new SparkContext(conf)
    */
    readInputString(args)
    
    //leer datos y transformarlos a LabeledPoint
    println("LEYENDO FICHERO...")
    val delimiter = FIELD_DELIMITER
    val file = sc.textFile(FILE_INPUT).map(line => line.split(delimiter))
    val data = file.map(row => 
      new LabeledPoint(
            row.last.toDouble, 
            Vectors.dense(row.take(row.length - 1).map(str => str.toDouble))
      )
    )
    
    val punto1 = data.first
    println("Primer punto del RDD[LabeledPoint]: ")
    println(" -Caracteristicas = " + punto1.features)
    println(" -Label = " + punto1.label)
    println
    
    println("INICIANDO CAIM...")
    val result: ArrayBuffer[(Int,(Float,Float))] = CAIM.discretizeData(data,sc, MEASURE_COLS)
    
    //TESTING
    println
    println("CAIM FINALIZADO...")
    println("RESULTADOS:")
    for(bin <- result) println("Dimension " + bin._1 + ": (" + bin._2._1 + ", " + bin._2._2 + ")")
    //END TESTING
    
    //transformar los datos
    
    //guardar datos nuevos
  }
  
    def readInputString(args:Array[String]): Unit = 
  {
    val total = args.length -1
    
    var found = false
    for (i <- 0 until total if !found)
    {
      if (args(i).equals("-FILE_INPUT"))
      {
        FILE_INPUT = args(i+1)
        found = true
      }
    }
    if (found == false)
			throw new Exception("Missing -FILE_INPUT");
    
//    found = false
//    for (i <- 0 until total if !found)
//    {
//      if (args(i).equals("-FILE_CP_OUTPUT"))
//      {
//        FILE_CP_OUTPUT = args(i+1)
//        found = true
//      }
//    }
//    if (found == false)
//			throw new Exception("Missing -FILE_CP_OUTPUT");
    
//    found = false
//    for (i <- 0 until total if !found)
//    {
//      if (args(i).equals("-FILE_DATA_OUTPUT"))
//      {
//        FILE_DATA_OUTPUT = args(i+1)
//        found = true
//      }
//    }
//    if (found == false)
//			throw new Exception("Missing -FILE_DATA_OUTPUT");
    
    found = false
    for (i <- 0 until total if !found)
    {
      if (args(i).equals("-MEASURE_COLS"))
      {
        MEASURE_COLS = args(i+1).toInt
        found = true
      }
    }
    if (found == false)
			throw new Exception("Missing -MEASURE_COLS");
    
    found = false
    for (i <- 0 until total if !found)
    {
      if (args(i).equals("-FIELD_DELIMITER"))
      {
        FIELD_DELIMITER = args(i+1).charAt(0)
        found = true
      }
    }

}
}