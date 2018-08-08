import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, Column}
import org.apache.spark.rdd._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.{row_number, udf, col}
import org.apache.spark.broadcast.Broadcast

object CAIM {
  
  def discretizeVariable(ss: SparkSession, data:DataFrame,
      targetName: String, variableName: String):
      (DataFrame, List[(Double, Int)]) =
	{
    val filteredData = mapDataFrame(data, variableName, targetName)
		val sc = ss.sparkContext
    val (frequenciesTable, nTargetLabels) = 
      FrequenciesTable.calculate(sc, filteredData, variableName, targetName)
		val cutPoints = Bins.calculate(sc, frequenciesTable, nTargetLabels)
		val discretizedData = discretizeData(variableName, cutPoints, data)
		return (discretizedData, cutPoints)
	}
  
  private def mapDataFrame(data: DataFrame, variableName: String,
      targetName: String): DataFrame = 
  {
    val dataFiltered = data.select(variableName, targetName)
    .withColumn(variableName, col(variableName).cast(DoubleType))
    .withColumn(targetName, col(targetName).cast(IntegerType))
    return(dataFiltered)
  }
  
	private def discretizeData(variableName: String, 
	    cutPoints: List[(Double, Int)], data: DataFrame): DataFrame =
	{
	  val discretizer = (value: Double) => getDiscreteValue(value, cutPoints.tail)
    val newColumn = udf(discretizer).apply(col(variableName))
    val discretizedData = data.withColumn(variableName, newColumn)
    return discretizedData
	}
	
	private def getDiscreteValue(value: Double, cutPoints: List[(Double,Int)]):
	    Int =
	      cutPoints match
  {
	  case Nil => throw new Exception("value " + value + "is out of bins range!")
	  case (cutPoint, discreteValue) :: tail if (cutPoint >= value) => 
	    discreteValue
	  case head :: tail => getDiscreteValue(value, tail)
  }

}
