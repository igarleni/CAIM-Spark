import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

object BestCandidate {
  
  def calculate(bins: List[((Double, Double), Double)],
  	    frequenciesTable: RDD[(Double, Array[Long])], nLabels: Int,
  	    globalCaim: Double):
  	    (Double, Double) =
  {
  	var bestScore = Double.MinValue
  	var bestPoint = -1.0
  
  	for(bin <- bins)
  	{
  		val pointInfluences = calculatePointInfluences(bin, frequenciesTable,
  		    nLabels)
  		val (bestBinPoint, bestBinScore) = searchBestScore(pointInfluences, 
  		    globalCaim, bins.length, bin)
  		if (bestBinScore > bestScore)
  		{
  			bestScore	= bestBinScore
  			bestPoint =	bestBinPoint
  		}
  	}
  	(bestPoint, bestScore)
  }
  
  private def calculatePointInfluences(bin: ((Double, Double), Double),
  	frequenciesTable: RDD[(Double, Array[Long])], nLabels: Int):
  	RDD[(Double, (Array[Long], Array[Long]))] =
  {
  	val min	= bin._1._1
  	val max	= bin._1._2
  	val binData = frequenciesTable.filter(point => (point._1 <= max) 
  			&& (point._1 > min))
  	val pointInfluences = binData.cartesian(binData).flatMap(points =>
	  {
  	  val point1 = points._1
  	  val point2 = points._2
  	  var point1Score = Array[(Double, (Array[Long], Array[Long]))]()
  	  var point2Score = Array[(Double, (Array[Long], Array[Long]))]()
  	  val emptyArray = Array.fill(nLabels)(0L)
  	  if (point1._1 > point2._1)
  	  {
  	    point1Score = Array((point1._1, (point2._2, emptyArray)))
  	    point2Score = Array((point2._1, (emptyArray, point1._2)))
  	  }
  	  else if (point1._1 < point2._1)
  	  {
  	    point1Score = Array((point1._1, (emptyArray, point2._2)))
  	    point2Score = Array((point2._1, (point1._2, emptyArray)))
  	  }
  	  point1Score ++ point2Score
  	})
  	pointInfluences
  }
  
  private def searchBestScore(
  	pointInfluences: RDD[(Double, (Array[Long], Array[Long]))], 
  	globalCaim: Double, nBins: Double, bin: ((Double, Double), Double)):
  	(Double, Double) =
  {
  	val	influencesCombiner = 
  	  (x:(Array[Long],Array[Long]) ,y:(Array[Long],Array[Long])) => 
  		  ((x._1,	y._1).zipped.map(_+_), (x._2, y._2).zipped.map(_+_))
	  val	partialScore	= globalCaim * nBins - bin._2
  	val	calculateScoreBins = 
  	  (point:(Double, (Array[Long], Array[Long])))	=> 
  	  {
  		val caimLeft = point._2._1.max / point._2._1.sum.toDouble
  		val caimRight = point._2._2.max / point._2._2.sum.toDouble
  		val caim = (caimLeft + caimRight + partialScore) / (nBins + 1)
  		(point._1, caim)
  	  }
  	val	pointsCaim = pointInfluences.reduceByKey(influencesCombiner)
  	.map(point => calculateScoreBins(point))
  	val	bestCaimPoint =	pointsCaim.max()(
  	    new Ordering[Tuple2[Double, Double]]() 
  	    {
  	      override def compare(x: (Double, Double), y: (Double, Double)): Int = 
  	        Ordering[Double].compare(x._2, y._2)
        }
    )
    bestCaimPoint
  }

}