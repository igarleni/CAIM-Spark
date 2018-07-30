import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd._

object FrequenciesTable {
    def calculate(sc: SparkContext, data: DataFrame, 
      variableName: String, targetName: String): 
      (RDD[(Long, Array[Long])], Int) =
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
  		val rowInformation = (rowId, targetFrequency)
  		rowInformation
  		}
		)
    return (frequenciesTable, nTargetLabels)
  }
}