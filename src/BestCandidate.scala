import org.apache.spark.rdd._

object BestCandidate {
  
  def calculate(bins: List[((Double, Double), Double)],
  	    frequenciesTable: RDD[(Double, Array[Long])], nLabels: Int,
  	    globalScore: Double):
  	    (Double, Double) =
  {
  	var bestScore = 0.0
  	var bestPoint = -1.0
  	for(bin <- bins)
  	{
  		val pointInfluences = calculatePointInfluences(bin, frequenciesTable,
  		    nLabels)
  		val (bestBinPoint, bestBinScore) = searchBestScore(pointInfluences, 
  		    globalScore, bins.length, bin)
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
		val	influencesCombiner = 
  	  (x:(Array[Long],Array[Long]) ,y:(Array[Long],Array[Long])) => 
  		  ((x._1,	y._1).zipped.map(_+_), (x._2, y._2).zipped.map(_+_))
	  val influencesReducer = 
	    (points: ((Double, Array[Long]), (Double, Array[Long]))) =>
	  {
  	  val point1 = points._1
  	  val point2 = points._2
  	  val emptyArray = Array.fill(nLabels)(0L)
  	  if(point1._1 != max)
  	  {
  		  if (point1._1 >= point2._1)
  			  Seq((point1._1, (point2._2, emptyArray)))
			  else
				  Seq((point1._1, (emptyArray, point2._2)))
  	  }
  	  else
  	    Seq[(Double, (Array[Long], Array[Long]))]()
  	}
  	val pointInfluences = binData.cartesian(binData).flatMap(influencesReducer)
  	.reduceByKey(influencesCombiner)
  	pointInfluences
  }
  
  private def searchBestScore(
  	pointInfluences: RDD[(Double, (Array[Long], Array[Long]))], 
  	globalScore: Double, nBins: Double, bin: ((Double, Double), Double)):
  	(Double, Double) =
  {
	  val	partialScore	= globalScore * nBins - bin._2
  	val	calculateBinsScore = 
  	  (point:(Double, (Array[Long], Array[Long])))	=> 
  	  {
  		val caimLeft = Math.pow(point._2._1.max, 2) / point._2._1.sum.toDouble
  		val caimRight = Math.pow(point._2._2.max, 2) / point._2._2.sum.toDouble
  		val caim = (caimLeft + caimRight + partialScore) / (nBins + 1)
  		(point._1, caim)
  	  }
  	val orderer = new Ordering[Tuple2[Double, Double]]() 
  	    {
  	      override def compare(x: (Double, Double), y: (Double, Double)): Int =
  	        Ordering[Double].compare(x._2, y._2)
        }
  	
  	val	pointsScores = pointInfluences.map(calculateBinsScore)
  	val	bestScorePoint =	pointsScores.max()(orderer)
    bestScorePoint
  }

}