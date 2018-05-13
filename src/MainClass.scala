import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.DataFrame 
import java.io._

object MainClass {
  
  def main(args:Array[String]): Unit = 
  {
    val sparkSession = generateSparkSession()
    val (delimiter, inputPath, inputFile, outputPath, outputFile, cutsOutputFile,
        targetColName) =  readInputStrings(args)
    val inputData = sparkSession.read.option("inferSchema", "true")
      .csv(inputPath + inputFile)
      
    val (outputData, cutPoints) = CAIM.discretizeAllVariables(inputData,
        sparkSession.sparkContext, targetColName)
    
    saveDataFrame(outputData, outputFile, delimiter)
    saveCutPoints(cutPoints, cutsOutputFile)
  }
  
    
  private def generateSparkSession(): SparkSession =
  {
    val sparkSession = SparkSession.builder().appName("Caim-Discretization")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    return (sparkSession)
  }
  
  
  private def readInputStrings(args:Array[String]) = 
  {
    val total = args.length - 1
    val parseOption:(String => String) = option =>
    ({
      val pos = args.indexOf(option)
      if (pos != -1 && pos < total)
        args(pos +1)
      else
        throw new Exception("Missing " + option)
    })
    
    val delimiter = parseOption("-FIELD_DELIMITER")
    val inputPath = parseOption("-INPUT_PATH")
    val inputFile = parseOption("-INPUT_FILE")
    val outputPath = parseOption("-OUTPUT_PATH")
    val outputFile = parseOption("-OUTPUT_FILE")
    val cutsOutputFile = parseOption("-OUTPUT_CPFILE")
    val targetColName = parseOption("-TARGET_COLUMN_NAME")
    
    (delimiter, inputPath, inputFile, outputPath, outputFile, cutsOutputFile,
        targetColName)
  }
    
  private def saveDataFrame(outputData: DataFrame, outputFile: String,
      delimiter: String) =
  {
    
  }
    
  private def saveCutPoints(cutPoints: Array[(Int, Array[Float])],
      cutsOutputFile: String) = 
  {
    val printWriter = new PrintWriter(new File(cutsOutputFile))
    val printCutPoint = (variable: (Int, Array[Float])) => 
      printWriter.write("Variable " + variable._1 +
          ", CutPoints = " + variable._2.mkString(", ") + "\n")
    cutPoints.foreach(printCutPoint(_))
    printWriter.close()
  }
  
}
