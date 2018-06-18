import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.row_number
import org.apache.spark.broadcast.Broadcast

object CAIM {
  
  def discretizeVariable(sc: SparkContext, data:DataFrame,
      targetName: String, variableName: String):
      (DataFrame, List[Float]) =
	{
    val dataFiltered = data.select(variableName, targetName)
    val dataSortedByVariable = dataFiltered.orderBy(variableName)
    val w = Window.orderBy("count")
    val dataWithId = dataSortedByVariable.withColumn("index", row_number().over(w))
    val (frequenciesTable, nLabels) = calculateFrequenciesTable(sc, dataWithId,
        variableName, targetName)
		val bins = CAIMmulti.calculateBins(sc, frequenciesTable, nLabels)
		val discretizedData = discretizeData(bins, data)
		return (discretizedData, bins)
	}
  
  private def calculateFrequenciesTable(sc: SparkContext, data: DataFrame, 
      variableName: String, targetName: String): 
      (RDD[(Long,(Float,Array[Long]))], Int) =
  {
		val uniqueTargetLabelsWithIndex = data.select(targetName).distinct
		    .collect.map(row => row(0)).zipWithIndex.toMap
    val bUniqueTargetLabelsWithIndex = sc.broadcast(uniqueTargetLabelsWithIndex)
		val nTargetLabels = uniqueTargetLabelsWithIndex.size
    return null
  }
  
	private def discretizeData(bins: List[Float], data: DataFrame):
	    DataFrame =
	{
	  return null // TODO
	}
	

}