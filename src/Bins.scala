import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer

object Bins
{
  
	def calculate(sc: SparkContext,
		frequenciesTable: RDD[(Double, Array[Long])], nLabels: Int):
		  List[Double] =
	{
		frequenciesTable.persist
		
		// Init CAIM and bins variables
		var globalCaim = Double.MinValue
		val selectedCutPoints = initSelectedCutPoints(frequenciesTable)
		var bins = ((selectedCutPoints(0) - 1 , selectedCutPoints(1)), 
		    globalCaim) :: Nil  // -1 so its included the first point too
		var numRemainingCPs = frequenciesTable.count -2  // min & max extracted
		var exitCondition = numRemainingCPs > 0 && bins.length < nLabels
		while(exitCondition)
		{
		  val (point, pointScore) =
		    BestCandidate.calculate(bins, frequenciesTable, nLabels, globalCaim)
			if(pointScore >	globalCaim)
			{
				selectedCutPoints += point
				numRemainingCPs	-= 1
				bins = updateBins(bins, point, frequenciesTable)
				globalCaim = pointScore
				exitCondition = numRemainingCPs > 0 && bins.length < nLabels
			}
			else
			  exitCondition = false
		}
		selectedCutPoints.toList
	}
	
	private def initSelectedCutPoints(
	    frequenciesTable: RDD[(Double, Array[Long])]): ArrayBuffer[Double] = 
	{
	  val selectedCutPoints = ArrayBuffer[Double]()
		selectedCutPoints += frequenciesTable.min()(
		    new Ordering[Tuple2[Double, Array[Long]]]() 
  	    {
  	      override def compare(x: (Double, Array[Long]),
  	          y: (Double, Array[Long])): Int = 
  	        Ordering[Double].compare(x._1, y._1)
        }
    )._1// min
		selectedCutPoints += frequenciesTable.max()(
		    new Ordering[Tuple2[Double, Array[Long]]]() 
  	    {
  	      override def compare(x: (Double, Array[Long]),
  	          y: (Double, Array[Long])): Int = 
  	        Ordering[Double].compare(x._1, y._1)
        }
    )._1// max
		selectedCutPoints
	}
	
	private def updateBins(bins: List[((Double, Double), Double)],
	    newCutPoint: Double, frequenciesTable: RDD[(Double, Array[Long])]): 
	    List[((Double, Double), Double)] =
  bins match
  {
  	case Nil => throw new Exception("End of bin list!")
  	case (cutPoints, _) :: tail if (newCutPoint < cutPoints._2) => 
  		insertCutPoint(cutPoints, newCutPoint, frequenciesTable) ::: tail
  	case bin::tail => bin :: updateBins(tail, newCutPoint, frequenciesTable)
  }
  
  private def insertCutPoint(cutPoints: (Double, Double), newCutPoint: Double,
      frequenciesTable: RDD[(Double, Array[Long])]): 
      List[((Double, Double), Double)] =
  {
	  val leftCutpoints = (cutPoints._1, newCutPoint)
	  val rightCutpoints = (newCutPoint, cutPoints._2)
    val leftScore = calculateBinScore(leftCutpoints, frequenciesTable)
    val rightScore = calculateBinScore(leftCutpoints, frequenciesTable)
  	val leftBin = (leftCutpoints, leftScore)
  	val rightBin = (rightCutpoints, rightScore)
  	leftBin :: rightBin :: Nil
  }
  
  private def calculateBinScore(cutpoints: (Double, Double),
      frequenciesTable: RDD[(Double, Array[Long])]): Double =
  {
    val binData = frequenciesTable.filter(point => (point._1 <= cutpoints._1) 
  			&& (point._1 > cutpoints._2))
  	val targetFrequency = binData.values.reduce( (frequency1, frequency2) => 
  	  (frequency1, frequency2).zipped.map(_+_))
  	val caimScore = targetFrequency.max / targetFrequency.sum.toDouble
    caimScore
  }
  
}


