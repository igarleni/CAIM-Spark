import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer

object CAIMmulti
{
	def caculateBins(dimension:Int,
		dimensionData: RDD[(Long,(Float, Array[Long]))],
		nLabels: Int): ArrayBuffer[(Int,(Float,Float))] =
	{
		dimensionData.persist

		// Inicializar variables
		var globalCaim = -Double.MaxValue
		val selectedCutPoints = ArrayBuffer[Long]()
		selectedCutPoints += dimensionData.first._1  // minimo
		selectedCutPoints += dimensionData.keys.max  // maximo
		val finalIDBins = ArrayBuffer[((Long,Long), Double)]()
		var nFinalBins = 1
		val fullRangeBin = ( (selectedCutPoints(0) - 1 , selectedCutPoints(1)),  // -1 para englobar todo
		    globalCaim)
		finalIDBins += fullRangeBin

		// Loop control variables
		var numRemainingCPs = dimensionData.count()
		numRemainingCPs -= 2  //minimo y maximo extraidos antes
		var nTempBins = nFinalBins + 1
		while(numRemainingCPs > 0 && nTempBins > nLabels)
		{
		  val bestCandidate = searchBestCandidate(finalIDBins, dimensionData,
		      nTempBins, nLabels, globalCaim)
		  
			if(bestCandidate._2 >	globalCaim)
			{
				 selectedCutPoints += bestCandidate._1._1
				var i	= 0
				var found	= false
				while	(i < finalIDBins.length	&& !found)
				{
					if (bestCandidate._1._1	< finalIDBins(i)._1._2)
					{
						found =	true
						val	actualBin =	finalIDBins(i)

						val	binLeft	= ((finalIDBins(i)._1._1 , bestCandidate._1._1), bestCandidate._1._2._1 )
						val	binRight = ((bestCandidate._1._1 ,	finalIDBins(i)._1._2), bestCandidate._1._2._2 )
						finalIDBins(i) = binRight
						finalIDBins.insert(i, binLeft)
						globalCaim = bestCandidate._2
						nFinalBins += 1
						numRemainingCPs	-= 1
					}
					i +=	1
				}
				 nTempBins += 1
			}
			else
			  nTempBins = nLabels + 1
		  
		}
		// Fix maxCutpoint Fix, so it	gets its true value
		finalIDBins(0) = ( (selectedCutPoints(0),	finalIDBins(0)._1._2), 0 )
		
		val result = ArrayBuffer[(Int,(Float,Float))]()
		//for (bin <- finalIDBins) result += (dimension, bin._1)
		result
	}
	
	def searchBestCandidate(finalIDBins: ArrayBuffer[((Long,Long), Double)],
	    dimensionData:RDD[(Long,(Float, Array[Long]))], nTempBins: Int,
	    nLabels: Int, globalCaim: Double):
	    Tuple2[Tuple2[Long,Tuple2[Double,Double]],Double] =
	{
	  // Temp variables for new candidate calculus
		var tempMaxCaim = -Double.MaxValue
		var tempBestCandidate:Tuple2[Long,Tuple2[Double,Double]] = null
		
		// Iterate over bins and calculate CAIM value locally
		for(bin <- finalIDBins)
		{
			val min	= bin._1._1
			val max	= bin._1._2
			val binData = dimensionData.filter(point => (point._1 <= max) 
					&& (point._1 > min))
				.map(item => (item._1, item._2._2))
			val pointInfluences = binData.flatMap(point => {
				val caimLeft = for(value <- min + 1 to point._1) 
					yield (point._1, (Array.fill[Long](nLabels)(0L),point._2))
				val caimRight = for(value <- point._1 + 1 to max) 
					yield (point._1, (point._2,Array.fill[Long](nLabels)(0L)))

				val caimRightArray = caimRight.toArray
				val caimLeftArray	= caimLeft.toArray
				caimRightArray ++ caimLeftArray
			})
			val	combiner = (x:Tuple2[Array[Long],Array[Long]] ,y:Tuple2[Array[Long],Array[Long]]) => 
			  (	((x._1,	y._1).zipped.map(_+_), (x._2, y._2).zipped.map(_+_)) )
			val	caimCalculator = (bins:	(Array[Long], Array[Long]))	=> (((bins._1.max /	bins._1.sum.toDouble),(bins._1.max / bins._1.sum.toDouble)))
			val	pointsPartialCaims = pointInfluences.reduceByKey(combiner).map((point => (point._1,	caimCalculator(point._2._1,point._2._2))))

			val	partialCaim	= globalCaim - bin._2
			val	getCaim	= (x:Tuple2[Long,Tuple2[Double,Double]]) =>	x._2._1	+ x._2._2 +	partialCaim	/ nTempBins
			val	bestCaimPoint =	pointsPartialCaims.max()(new Ordering[Tuple2[Long, Tuple2[Double,Double]]]() {
				override	def	compare(x: (Long, (Double, Double)), y:	(Long, (Double,	Double))): Int = 
				{
				val xCaim = getCaim(x)
				val yCaim = getCaim(y)
					Ordering[Double].compare(xCaim,	yCaim)
				}
			})
			val bestCaim = getCaim(bestCaimPoint)
			if (bestCaim > tempMaxCaim)
			{
				tempMaxCaim	= bestCaim
				tempBestCandidate =	bestCaimPoint
			}
		}
		(tempBestCandidate, tempMaxCaim)
	}
	
}
