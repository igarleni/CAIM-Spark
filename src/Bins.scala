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
		    globalCaim) :: Nil // -1 so its included the first point too
		var numRemainingCPs = selectedCutPoints(1) - 2  // min & max were extracted
		var exitCondition = numRemainingCPs > 0 && bins.length + 1 > nLabels
		while(exitCondition)
		{
		  val (point, pointScore) =
		    BestCandidate.calculate(bins, frequenciesTable, nLabels, globalCaim)
			if(pointScore >	globalCaim)
			{
				selectedCutPoints += point
				numRemainingCPs	-= 1
				bins = updateBins(bins, point)
				globalCaim = pointScore
				exitCondition = numRemainingCPs > 0 && bins.length + 1 > nLabels
			}
			else
			  exitCondition = false
		}
		selectedCutPoints(0) = selectedCutPoints(0) + 1  // Fix the first cut point
		selectedCutPoints.toList
	}
	
	private def initSelectedCutPoints(
	    frequenciesTable: RDD[(Double, Array[Long])]): ArrayBuffer[Double] = 
	{
	  val selectedCutPoints = ArrayBuffer[Double]()
		selectedCutPoints += frequenciesTable.first._1  // min
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
	    newCutPoint: Double): List[((Double, Double), Double)] =
  bins match
  {
  	case Nil => throw new Exception("End of bin list!")
  	case (cutPoints, _) :: tail if (newCutPoint < cutPoints._2) => 
  		insertCutPoint(cutPoints, newCutPoint) ::: tail
  	case bin::tail => bin :: updateBins(tail, newCutPoint)
  }
  
  private def insertCutPoint(cutPoints: (Double, Double), newCutPoint: Double):
    List[((Double, Double), Double)] =
  {
	  val leftCutpoints = (cutPoints._1, newCutPoint)
	  val rightCutpoints = (newCutPoint, cutPoints._2)
    val leftScore = calculateBinScore(leftCutpoints)
    val rightScore = calculateBinScore(leftCutpoints)
  	val leftBin = (leftCutpoints, leftScore)
  	val rightBin = (rightCutpoints, rightScore)
  	leftBin :: rightBin :: Nil
  }
  
  private def calculateBinScore(cutpoints: (Double, Double)): Double =
  {
    0.0 //TODO calcular bin score
  }
  
}


