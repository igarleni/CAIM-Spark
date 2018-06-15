import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}

import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast

object CAIM {
  
  def discretizeAllVariables(data:DataFrame, sparkContext: SparkContext, 
      targetName: String): (DataFrame, Map[String, List[Float]]) =
	{
    
		val uniqueTargetLabelsWithIndex = data.select(targetName).distinct
		    .collect.map(row => row(0)).zipWithIndex.toMap
		val bUniqueTargetLabelsWithIndex = sparkContext
		    .broadcast(uniqueTargetLabelsWithIndex)
		    
		val nLabels = uniqueTargetLabelsWithIndex.size
		val variablesNames = data.columns
		
		//TODO seguir aqui, convertir for en tail recursive y transfomrar RDD a DataFrame
		var bins = Map[String, Array[Float]]()
		
		for (variableName <- variablesNames if variableName != targetName)
		{
		  val variableAndTargetData = data.select(variableName, targetName)
		  val sorteduniqueData = Sorter.getSortedDistinctData(variableAndTargetData, 
		      bUniqueTargetLabelsWithIndex)
			val variableData = sorteduniqueData.zipWithIndex().map(_.swap)
			bins = bins + (variableName -> CAIMmulti.caculateBins(variableData,
			    nLabels))
		}
		
		val discretizedData = discretizeData(bins, data, variablesNames, targetName)
		return (discretizedData, bins)
	}
  
  
	private def discretizeData(bins: Map[String, Array[Float]],
	    data: DataFrame, variablesNames: Array[String], targetName: String):
	    DataFrame =
	{
	  for (variableName <- variablesNames if variableName != targetName)
		{
	    
		}
	  return null // TODO
	}
	

}