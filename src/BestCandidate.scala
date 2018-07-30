import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

object BestCandidate {
  
  def calculate(bins: List[((Long,Long), Double)],
  	    frequenciesTable: RDD[(Long, Array[Long])], nLabels: Int,
  	    globalCaim: Double):
  	    ((Long, (Double, Double)), Double) =
  {
  	var bestCaim = Double.MinValue
  	var bestPoint: (Long, (Double,Double)) = null
  
  	for(bin <- bins)
  	{
  		val pointInfluences = calculatePointInfluences(bin, frequenciesTable,
  		    nLabels)
  		val pointsPartialCaim = calculatePartialCaim(pointInfluences)
  		val (bestBinPoint, bestBinCaim) = getBestPoint(pointsPartialCaim,
  		    globalCaim, bins.length, bin)
  
  		if (bestBinCaim > bestCaim)
  		{
  			bestCaim	= bestCaim
  			bestPoint =	bestBinPoint
  		}
  	}
  	(bestPoint, bestCaim)
  }
  
  private def calculatePointInfluences(bin: ((Long, Long), Double),
  	frequenciesTable: RDD[(Long, Array[Long])], nLabels: Int):
  	RDD[(Long, (Array[Long], Array[Long]))] =
  {
  	val min	= bin._1._1
  	val max	= bin._1._2
  	val binData = frequenciesTable.filter(point => (point._1 <= max) 
  			&& (point._1 > min))
  	val pointInfluences = binData.flatMap(point => {
  		val caimLeft = for(pointId <- min + 1 to point._1) 
  			yield (pointId, (Array.fill[Long](nLabels)(0L), point._2))
  		val caimRight = for(pointId <- point._1 + 1 to max) 
  			yield (pointId, (point._2, Array.fill[Long](nLabels)(0L)))
  
  		val caimRightArray = caimRight.toArray
  		val caimLeftArray	= caimLeft.toArray
  		caimRightArray ++ caimLeftArray
  	})
  	pointInfluences
  }
  
  private def calculatePartialCaim(
  	pointInfluences: RDD[(Long, (Array[Long], Array[Long]))]):
  	RDD[(Long, (Double, Double))] =
  {
  	val	influencesCombiner = 
  	  (x:(Array[Long],Array[Long]) ,y:(Array[Long],Array[Long])) => 
  		  ((x._1,	y._1).zipped.map(_+_), (x._2, y._2).zipped.map(_+_))
  	val	calculateCaimBins = 
  	  (point:(Long, (Array[Long], Array[Long])))	=> 
  	  {
  		val caimLeft = point._2._1.max / point._2._1.sum.toDouble
  		val caimRight = point._2._2.max / point._2._2.sum.toDouble
  		(point._1, (caimLeft, caimRight))
  	  }
  	val	pointsPartialCaim = pointInfluences.reduceByKey(influencesCombiner)
  	.map((point => calculateCaimBins(point)))
  	pointsPartialCaim
  }
  
  private def getBestPoint(pointsPartialCaim: RDD[(Long, (Double, Double))],
      globalCaim: Double, nBins: Double, bin: ((Long, Long), Double)): 
      ((Long, (Double, Double)), Double) =
  {
  	val	partialCaim	= globalCaim * nBins - bin._2
  	val	calculateCaimPoint	= (x:(Long,(Double,Double))) =>
  	  x._2._1	+ x._2._2 +	partialCaim	/ (nBins + 1)
  	val	bestCaimPoint =	pointsPartialCaim.max()
  	(
  		new Ordering[(Long, (Double,Double))]() 
  		{
  		  override	def	compare(x: (Long, (Double, Double)), 
  			y: (Long, (Double,	Double))): Int = 
  			{
  			val xCaim = calculateCaimPoint(x)
  			val yCaim = calculateCaimPoint(y)
  			Ordering[Double].compare(xCaim,	yCaim)
  	  }
  		}
  	)
  	val bestCaim = calculateCaimPoint(bestCaimPoint)
  	(bestCaimPoint, bestCaim)
  }

}