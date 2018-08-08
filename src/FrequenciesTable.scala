import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd._

object FrequenciesTable {
  
  def calculate(sc: SparkContext, data: DataFrame, variableName: String,
      targetName: String): (RDD[(Double, Array[Long])], Int) =
  {
		val targetLabelsWithIndex = data.select(targetName).distinct
		    .collect.map(row => row.getAs[Int](targetName)).zipWithIndex.toMap
		val bTargetLabels = sc.broadcast(targetLabelsWithIndex)
		val nTargetLabels = targetLabelsWithIndex.size
		val targetFrequenciesCalculator = (rowData:Row) => 
	  {
		  val targetFrequency = Array.fill[Long](nTargetLabels)(0L)
  		val targetValue = rowData.getAs[Int](targetName)
  		targetFrequency(bTargetLabels.value(targetValue)) = 1L
  		val variableData = rowData.getAs[Double](variableName)
  		val rowInformation = (variableData, targetFrequency)
  		rowInformation
		}
		val frequencyCombiner = (targetFrequency1: Array[Long], 
		    targetFrequency2: Array[Long]) => (
		        (targetFrequency1, targetFrequency2).zipped.map(_+_)
        )
    
		val frequenciesTable = data.rdd.map(targetFrequenciesCalculator)
		    .reduceByKey(frequencyCombiner)
    return (frequenciesTable, nTargetLabels)
  }
}
