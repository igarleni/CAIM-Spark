import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.broadcast.Broadcast
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object CAIMmulti {
	var sc: SparkContext = null
	
	def discretizeData(data: RDD[LabeledPoint], sPc:SparkContext, cols:Int): ArrayBuffer[(Int,(Float,Float))] =
	{
		sc = sPc
		//obtenemos los labels de la variable clase
		val labels2Int = data.map(_.label).distinct.collect.zipWithIndex.toMap
		val nLabels = labels2Int.size
		//creamos un map de los index labels y los cuenta, obteniendo el numero de apariciones
		//de cada distinct
		val bLabels2Int = sc.broadcast(labels2Int)
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
		
		//los ordenamos
		val sortedValues = getSortedDistinctValues(bclassDistrib, featureValues).persist
		
		//Aplicar CAIM a cada dimension
		val bins = ArrayBuffer[(Int,(Float,Float))]()
		for (dimension <- 0 until cols)
		{
			val dimensionData = sortedValues.filter(_._1._1 == dimension).map({case ((dim,value),hlabels) => (value, hlabels)}).zipWithIndex().map(_.swap)
			bins ++= caimDimensionDiscretization(dimension,dimensionData, nLabels)
		}
		bins
	}
	
	def caimDimensionDiscretization(dimension:Int, dimensionData: RDD[(Long,(Float, Array[Long]))], nLabels: Int): ArrayBuffer[(Int,(Float,Float))] =
	{
	  dimensionData.persist
	  
		// Inicializar variables
	  var globalCaim = -Double.MaxValue
		val selectedCutPoints = ArrayBuffer[Long]()
		selectedCutPoints += dimensionData.first._1 // minimo
		selectedCutPoints += dimensionData.keys.max //maximo
		val finalIDBins = ArrayBuffer[((Long, Long), Double)] ()
		var nFinalBins = 1
		val fullRangeBin = ( (selectedCutPoints(0) - 1 , selectedCutPoints(1)), globalCaim)  // -1 para englobar todo
		finalIDBins += fullRangeBin
		
		// Loop control variables
		var numRemainingCPs = dimensionData.count()
		numRemainingCPs -= 2 //minimo y maximo extraidos antes
		var exit = false
		
		// Main loop
		while(numRemainingCPs > 0 && !exit)
		{	
		  // Temp variables for new candidate calculus
			var tempMaxCaim = -Double.MaxValue
			var tempBestCandidate = 0f
			var tempBestLeftCaim = 0.0
			var tempBestRightCaim = 0.0
			val nTempBins = nFinalBins + 1
			
			// Iterate over bins and calculate CAIM value locally
			for(bin <- finalIDBins)
			{
				val min = bin._1._1
				val max = bin._1._2
				val binData = dimensionData.filter(point => (point._1 <= max) && (point._1 > min)).map(item => (item._1, item._2._2))
				val pointInfluences = binData.flatMap(point => {
				  val caimLeft = for(value <- min + 1 to point._1) yield (point._1, (Array.fill[Long](nLabels)(0L),point._2))
				  val caimRight = for(value <- point._1 + 1 to max) yield (point._1, (point._2,Array.fill[Long](nLabels)(0L)))
				  
				  val caimRightArray = caimRight.toArray
				  val caimLeftArray = caimLeft.toArray
				  caimRightArray ++ caimLeftArray
				})
				val combiner = (x:Tuple2[Array[Long],Array[Long]] ,y:Tuple2[Array[Long],Array[Long]]) => 
				  ( ((x._1, y._1).zipped.map(_+_), (x._2, y._2).zipped.map(_+_)) )
				val caimCalculator = (bins: (Array[Long], Array[Long])) => (((bins._1.max / bins._1.sum.toDouble),(bins._1.max / bins._1.sum.toDouble)))
				val pointsPartialCaims = pointInfluences.reduceByKey(combiner).map((point => (point._1, caimCalculator(point._2._1,point._2._2))))
				
				//TODO include lateral bins on bestCaim
				val bestCaim = pointsPartialCaims.max()(new Ordering[Tuple2[Long, Tuple2[Double,Double]]]() {
          override def compare(x: (Long, (Double, Double)), y: (Long, (Double, Double))): Int = 
          Ordering[Double].compare(x._2._1, y._2._2)
        })
        if (bestCaim._2 > tempMaxCaim)
			  {
			    tempMaxCaim = bestCaim._2
			    tempBestCandidate = bestCaim._1
			  }
			}
			
			// Check if best CAIM is 
			if(tempMaxCaim > globalCaim || nTempBins < nLabels)
			{
			  selectedCutPoints += tempBestCandidate
      	var i = 0
      	var found = false
      	// TODO: calculate new CAIM bins
      	while (i < finalBins.length && !found)
      	{
      	  if (tempBestCandidate < finalBins(i)._1._2)
      	  {
        		found = true
        		val actualBin = finalBins(i)
        		
        		val binLeft = ((finalBins(i)._1._1 , tempBestCandidate), tempBestLeftCaim )
        		val binRight = ((tempBestCandidate , finalBins(i)._1._2), tempBestRightCaim )
        		finalBins(i) = binRight
        		finalBins.insert(i, binLeft)
        		globalCaim = tempMaxCaim
        		nFinalBins += 1
        		numRemainingCPs -= 1
      	  }
      	  i += 1
      	}
			}
			else if (nTempBins >= nLabels - 1)
			  exit = true
		}
		// Fix maxCutpoint Fix, so it gets its true value
    finalBins(0) = ( (selectedCutPoints(0), finalBins(0)._1._2), 0 )
    //TODO Transform ID values to real values
    // add dimension ID to bins
    val result = ArrayBuffer[(Int,(Float,Float))]()
    result ++= (for (bin <- finalBins) yield (dimension, bin._1))
    
    result
	}

	  //genera (Dimension, Valor), [countlabelClase1, countlabelClase2, ... ]
  def getSortedDistinctValues(
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
  
  
  def addZerosIfNeeded(nonZeros: RDD[((Int, Float), Array[Long])],
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