import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer

object Bins
{
  
	def calculate(sc: SparkContext,
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
		    BestCandidate.calculate(bins, frequenciesTable, nLabels, globalCaim)
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


