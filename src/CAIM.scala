import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.row_number
import org.apache.spark.broadcast.Broadcast

object CAIM {
  
  def discretizeVariable(ss: SparkSession, data:DataFrame,
      targetName: String, variableName: String):
      (DataFrame, List[Float]) =
	{
    val sc = ss.sparkContext
    val filteredData = mapDataFrame(ss, data, variableName, targetName)
    filteredData.show
    filteredData.printSchema
    val (frequenciesTable, nTargetLabels) = 
      calculateFrequenciesTable(sc, filteredData, variableName, targetName)
    frequenciesTable.collect.foreach(println)
		val bins = CAIMmulti.calculateBins(sc, frequenciesTable, nTargetLabels)
//		val discretizedData = discretizeData(bins, data)
//		return (discretizedData, bins)
    return null
	}
  
  private def mapDataFrame(ss: SparkSession, data: DataFrame,
      variableName: String, targetName: String): DataFrame = 
  {
    val dataFiltered = data.select(variableName, targetName)
    val dataSortedByVariable = dataFiltered.orderBy(variableName)
    val w = Window.orderBy(variableName)
    val dataWithId = dataSortedByVariable
    .withColumn("index", row_number().over(w).cast(LongType))
    return(dataWithId)
  }
  
  private def calculateFrequenciesTable(sc: SparkContext, data: DataFrame, 
      variableName: String, targetName: String): 
      (RDD[(Long, (Double, Array[Long]))], Int) =
  {
		val targetLabelsWithIndex = data.select(targetName).distinct
		    .collect.map(row => row.getAs[Int](targetName)).zipWithIndex.toMap
		val bTargetLabels = sc.broadcast(targetLabelsWithIndex)
		val nTargetLabels = targetLabelsWithIndex.size
		val frequenciesTable = data.rdd.map( (rowData:Row) => {
		  val targetFrequency = Array.fill[Long](nTargetLabels)(0L)
  		val targetValue = rowData.getAs[Int](targetName)
  		targetFrequency(bTargetLabels.value(targetValue)) = 1L
  		val rowId = rowData.getAs[Long]("index")
  		val variableData = rowData.getAs[Double](variableName)
  		val rowInformation = (rowId, (variableData, targetFrequency))
  		rowInformation
  		}
		)
    return (frequenciesTable, nTargetLabels)
  }
  
	private def discretizeData(bins: List[Float], data: DataFrame):
	    DataFrame =
	{
	  return null // TODO
	}

}