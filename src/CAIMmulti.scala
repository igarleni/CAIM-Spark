import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer

object CAIMmulti
{
  
	def calculateBins(sc: SparkContext,
		frequenciesTable: RDD[(Long, Array[Long])], nLabels: Int):
		  List[Long] =
	{
		frequenciesTable.persist
		// Init CAIM and bins variables
		var globalCaim = Double.MinValue
		val selectedCutPoints = initSelectedCutPoints(frequenciesTable)
		var bins = ((selectedCutPoints(0) - 1 , selectedCutPoints(1)), 
		    globalCaim) :: Nil // -1 so its included the first point too
		var numRemainingCPs = selectedCutPoints(1) - 2  // min & max were extracted
		var exitCondition = numRemainingCPs > 0 && bins.length + 1 > nLabels
		while(exitCondition)
		{
		  val ((pointID, pointPartialCaim), pointCaim) =
		    searchBestCandidate(bins, frequenciesTable, nLabels, globalCaim)
			if(pointCaim >	globalCaim)
			{
				selectedCutPoints += pointID
				numRemainingCPs	-= 1
				bins = updateBins(bins, pointID, pointPartialCaim)
				globalCaim = pointCaim
				exitCondition = numRemainingCPs > 0 && bins.length + 1 > nLabels
			}
			else
			  exitCondition = false
		}
		selectedCutPoints(0) = selectedCutPoints(0) + 1  // Fix the first cut point
		selectedCutPoints.toList
	}
	
	def searchBestCandidate(bins: List[((Long,Long), Double)],
	    frequenciesTable: RDD[(Long, Array[Long])], nLabels: Int,
	    globalCaim: Double):
	    ((Long, (Double, Double)), Double) =
	{
	  // Temp variables for new candidate calculus
		var maxCaim = -Double.MaxValue
		var tempBestCandidate: (Long, (Double,Double)) = null
		
		// Iterate over bins and calculate CAIM value
		//TODO meter en una funcion tail recursive de listas
		for(bin <- bins)
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
			val	influencesCombiner = 
			  (x:(Array[Long],Array[Long]) ,y:(Array[Long],Array[Long])) => 
			      ((x._1,	y._1).zipped.map(_+_), (x._2, y._2).zipped.map(_+_))
			val	calculateCaimBins = 
			  (point:(Long, (Array[Long], Array[Long])))	=> 
			  {
			    val caimLeft = point._2._1.max / point._2._1.sum.toDouble
			    val caimRight = point._2._1.max / point._2._1.sum.toDouble
			    (point._1, (caimLeft, caimRight))
			  }
			val	pointsPartialCaim = pointInfluences.reduceByKey(influencesCombiner)
			.map((point => calculateCaimBins(point)))

			val	partialCaim	= globalCaim - bin._2
			val	calculateCaimPoint	= (x:(Long,(Double,Double))) =>
			  x._2._1	+ x._2._2 +	partialCaim	/ (bins.length + 1)
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
			if (bestCaim > maxCaim)
			{
				maxCaim	= bestCaim
				tempBestCandidate =	bestCaimPoint
			}
		}
		(tempBestCandidate, maxCaim)
	}
	
	def initSelectedCutPoints(frequenciesTable: RDD[(Long, Array[Long])]):
	ArrayBuffer[Long] = 
	{
	  val selectedCutPoints = ArrayBuffer[Long]()
		selectedCutPoints += 1  // min
		val nRows = frequenciesTable.count()
		selectedCutPoints += nRows  // max
		selectedCutPoints
	}
	
	def updateBins(bins: List[((Long, Long), Double)], newCutPoint: Long, 
	    newCaims: (Double, Double)): List[((Long, Long), Double)] =
  bins match
  {
  	case Nil => throw new Exception("Empty bin list!")
  	case (cutPoints, _) :: tail if (newCutPoint < cutPoints._2) => 
  		insertCutPoint(cutPoints, newCutPoint, newCaims) ::: tail
  	case bin::tail => bin :: updateBins(tail, newCutPoint, newCaims)
  }
  
  def insertCutPoint(cutPoints: (Long, Long), newCutPoint: Long,
      newCaims: (Double, Double)): List[((Long, Long), Double)] =
  {
  	val binLeft = ((cutPoints._1, newCutPoint), newCaims._1)
  	val binRight = ((newCutPoint, cutPoints._2), newCaims._2)
  	binLeft :: binRight :: Nil
  }
	
}


