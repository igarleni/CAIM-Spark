import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import java.io._

object MainClass {
  
  def main(args:Array[String]): Unit = 
  {
    val sc = generateSparkContext()
    val (delimiter, inputFile, outputFile, cutsOutputFile, nColumns) = 
      readInputString(args)
    val rawData = sc.textFile(inputFile)
    val data = transformToLabeledPoint(rawData, delimiter)
    
    // val (outputData, cutPoints) = CAIM.discretizeAllVariables(data, sc, nColumns)
    
    saveLabeledPoint(data, outputFile, delimiter)
    // saveCutPoints(cutPoints, cutsOutputFile)
  }
  
  
  private def saveLabeledPoint(data: RDD[LabeledPoint], outputFile: String,
      delimiter: String) =
  {
    val stringRDD = data.map(point => 
      point.features.toArray.mkString(delimiter)
      + delimiter + point.label.toString)
    stringRDD.saveAsTextFile(outputFile)
  }
  
    
  private def saveCutPoints(cutPoints: Array[(Int, Array[Float])],
      cutsOutputFile: String) = 
  {
    val printWriter = new PrintWriter(new File(cutsOutputFile))
    cutPoints.foreach(variable => 
      printWriter.write("Variable " + variable._1 +
          ", CutPoints = " + variable._2.mkString(", ")))
  }
  
  
  private def generateSparkContext(): SparkContext =
  {
    val conf = new SparkConf()
    conf.set("spark.cores.max", "20")
    conf.set("spark.executor.memory", "6g")
    conf.set("spark.kryoserializer.buffer.max", "512")
    conf.setAppName("CAIMdiscretization")
    
    return (new SparkContext(conf))
  }
  
  
  private def transformToLabeledPoint(rawData: RDD[String], delimiter: String):
    RDD[LabeledPoint] =
  {
    val splittedData = rawData.map(line => line.split(delimiter))
    val labeledPointData = splittedData.map(row => 
      new LabeledPoint(
            row.last.toDouble, 
            Vectors.dense(row.take(row.length - 1).map(str => str.toDouble))
      )
    )
    return(labeledPointData)
  }
  
  
  private def readInputString(args:Array[String]) = 
  {
    val total = args.length -1
    val delimiter = parseOption(args, total, "-FIELD_DELIMITER")
    val inputFile = parseOption(args, total, "-FILE_INPUT")
    val outputFile = parseOption(args, total, "-FILE_DATA_OUTPUT")
    val cutsOutputFile = parseOption(args, total, "-FILE_CP_OUTPUT")
    val nColumns = parseOption(args, total, "-MEASURE_COLS").toInt
    
    (delimiter, inputFile, outputFile, cutsOutputFile, nColumns)
  }
  
  
  private def parseOption(args: Array[String], total: Int, option: String):
    String =
  {
    for (i <- 0 until total if args(i).equals(option))
        return(args(i+1))
        
		throw new Exception("Missing " + option)
  }
}