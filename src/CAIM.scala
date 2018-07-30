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
    val filteredData = mapDataFrame(data, variableName, targetName)
		val sc = ss.sparkContext
    val (frequenciesTable, nTargetLabels) = 
      FrequenciesTable.calculate(sc, filteredData, variableName, targetName)
		val bins = Bins.calculate(sc, frequenciesTable, nTargetLabels)
//		val discretizedData = discretizeData(bins, data)
//		return (discretizedData, bins)
    return null
	}
  
  private def mapDataFrame(data: DataFrame, variableName: String,
      targetName: String): DataFrame = 
  {
    val dataFiltered = data.select(variableName, targetName)
    val dataSortedByVariable = dataFiltered.orderBy(variableName)
    val w = Window.orderBy(variableName)
    val dataWithId = dataSortedByVariable
    .withColumn("index", row_number().over(w).cast(LongType))
    return(dataWithId)
  }
  

  
	private def discretizeData(bins: List[Float], data: DataFrame):
	    DataFrame =
	{
	  return null // TODO
	}

}