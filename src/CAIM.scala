import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.broadcast.Broadcast
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object CAIM {
  def discretizeAllVariables(data:RDD[LabeledPoint], sc:SparkContext, cols:Int):
		(RDD[LabeledPoint], Array[(Int, Array[Float])]) =
	{
		val labels2Int = data.map(_.label).distinct.collect.zipWithIndex.toMap
		val nLabels = labels2Int.size
		val bLabels2Int = sc.broadcast(labels2Int)

		// ((Dimension:Int, Value:Float),	TargetContingency:Array[Long])
		val sortedDistinctValues:RDD[((Int, Float), Array[Long])] = Sorter.sortValues(data, sc, 
			bLabels2Int, nLabels)
		
		val bins = ArrayBuffer[(Int,(Float,Float))]()
		for (dimension <- 0 until cols)
		{
			val dimensionData = sortedDistinctValues.filter(_._1._1 == dimension)
				.map({case ((dim,value),hlabels) => (value,	hlabels)}).zipWithIndex().map(_.swap)
			dimensionData.collect().foreach(println(_))
			//bins ++= CAIMmulti.caculateBins(dimension,dimensionData, nLabels)
		}
		
		val discretizedData = discretizeData(bins, sortedDistinctValues.map(_._1))
		return (discretizedData, Array[(Int, Array[Float])]())
	}


	def discretizeVariables(variableData:RDD[(Float, Array[Long])],
		sc: SparkContext, variablesToDiscretize:Array[Int]): RDD[LabeledPoint] =
	{
		return null // TODO
	}

	def discretizeData(bins: ArrayBuffer[Tuple2[Int,Tuple2[Float,Float]]],
	    data:RDD[Tuple2[Int,Float]]): RDD[LabeledPoint] =
	{
	  return null // TODO
	}

}