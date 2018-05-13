import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext

import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast
import scala.collection.Map

object Sorter {
  
  def getSortedDistinctData(data: DataFrame,
      bUniqueTargetLabelsWithIndex: 
      Broadcast[scala.collection.immutable.Map[Any, Int]]):
      RDD[(Float, Array[Long])] =
  {
    return null //TODO data.dtypes?
  }
  
  	def sortValues(data:RDD[LabeledPoint], sc:SparkContext,
		bLabels2Int:Broadcast[scala.collection.immutable.Map[Double, Int]], nLabels:Int): 
		RDD[((Int, Float), Array[Long])] =
	{
		val classDistrib = data.map(d => bLabels2Int.value(d.label)).countByValue()
		val bclassDistrib = sc.broadcast(classDistrib)
		val featureValues =
        data.flatMap({
			case LabeledPoint(label, dv: DenseVector) =>
				val c = Array.fill[Long](nLabels)(0L)
				c(bLabels2Int.value(label)) = 1L
				for (i <- dv.values.indices) yield ((i, dv(i).toFloat), c)
			case LabeledPoint(label, sv: SparseVector) =>
				val c = Array.fill[Long](nLabels)(0L)
				c(bLabels2Int.value(label)) = 1L
				for (i <- sv.indices.indices) yield ((sv.indices(i), sv.values(i).toFloat), c)
		})
		val sortedValues = getSortedDistinctValues(bclassDistrib, featureValues)
		
		sortedValues
	}
  
  
  private def getSortedDistinctValues(
    bclassDistrib: Broadcast[Map[Int, Long]],
    featureValues: RDD[((Int, Float), Array[Long])]): RDD[((Int, Float), Array[Long])] = {
    
    val nonZeros: RDD[((Int, Float), Array[Long])] =
      featureValues.map(y => (y._1._1 + "," + y._1._2, y._2)).reduceByKey { case (v1, v2) =>
      (v1, v2).zipped.map(_ + _)
    }.map(y => {
      val s = y._1.split(",")
      ((s(0).toInt, s(1).toFloat), y._2)
    })

    val zeros = addZerosIfNeeded(nonZeros, bclassDistrib)
    val distinctValues = nonZeros.union(zeros)

    // Sort these values to perform the boundary points evaluation
    val start = System.currentTimeMillis()
    val result = distinctValues.sortByKey()
    //println("done sortByKey in " + (System.currentTimeMillis() - start))
    result
  }
  
  
  private def addZerosIfNeeded(nonZeros: RDD[((Int, Float), Array[Long])],
      bclassDistrib: Broadcast[Map[Int, Long]]): RDD[((Int, Float), Array[Long])] = {
    nonZeros
      .map { case ((k, p), v) => (k, v) }
      .reduceByKey { case (v1, v2) => (v1, v2).zipped.map(_ + _) }
      .map { case (k, v) =>
        val v2 = for (i <- v.indices) yield bclassDistrib.value(i) - v(i)
        ((k, 0.0F), v2.toArray)
      }.filter { case (_, v) => v.sum > 0 }
  }

}