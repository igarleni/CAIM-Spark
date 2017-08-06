import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object MainClass {
  private var FILE_INPUT:String = null
  private var FILE_CP_OUTPUT:String = null
  private var FILE_DATA_OUTPUT:String = null
  private var MEASURE_COLS:Int = 0
  private var FIELD_DELIMITER:String = ";"
  
  def main(args:Array[String]): Unit = 
  {
    val conf = new SparkConf()
    conf.set("spark.cores.max", "20")
    conf.set("spark.executor.memory", "6g")
    conf.set("spark.kryoserializer.buffer.max", "512")
    conf.setAppName("IPDdiscretization")
    val sc = new SparkContext(conf)
    
    println("----------------App init----------------")
    readInputString(args)
    //leer datos y transformarlos
    val file = sc.textFile(FILE_INPUT)
    val intermediate = file.map(line => line.split(FIELD_DELIMITER))
    val data:RDD[LabeledPoint] = file.map(line => line.split(FIELD_DELIMITER)).map(line => new LabeledPoint(line(line.length -1).toDouble, Vectors.dense(line.slice(0,(line.length -1)).map(_.toDouble) ) ) )
    
    //Aplicar CAIM y obtener los bins
    //val bins:RDD[(Int,(Float,Float))] = CAIM.discretizeData(data,sc, MEASURE_COLS)
    val result = CAIM.discretizeData(data,sc, MEASURE_COLS)
    
    //Print data
    println("Tamaño del resultado " + result.length)
    for(bin <- result) println("Dimension " + bin._1 + ": (" + bin._2._1 + ", " + bin._2._2 + ")")
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
    
    found = false
    for (i <- 0 until total if !found)
    {
      if (args(i).equals("-FILE_CP_OUTPUT"))
      {
        FILE_CP_OUTPUT = args(i+1)
        found = true
      }
    }
    if (found == false)
			throw new Exception("Missing -FILE_CP_OUTPUT");
    
    found = false
    for (i <- 0 until total if !found)
    {
      if (args(i).equals("-FILE_DATA_OUTPUT"))
      {
        FILE_DATA_OUTPUT = args(i+1)
        found = true
      }
    }
    if (found == false)
			throw new Exception("Missing -FILE_DATA_OUTPUT");
    
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
        FIELD_DELIMITER = args(i+1)
        found = true
      }
    }

}
}