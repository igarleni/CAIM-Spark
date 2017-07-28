import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.broadcast.Broadcast
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object CAIM {
  //Bins variables = ( (CutPointInit, CutPointEnd)  ,  ClassHistogram )
  var finalBins = ArrayBuffer[((Float, Float), Array[Long])] ()
  var nFinalBins = 1
  var finalCuPoints = 2
  //temporal Bins = ( CAIM , Class histogram )
  var tempBins = ArrayBuffer[(Float, Array[Long])] ()
  var nTempBins = 2
  var actualTempBin = -1

    
  //def discretizeData(data: RDD[LabeledPoint], sc:SparkContext, cols:Int): RDD[(Int,(Float,Float))] =
  def discretizeData(data: RDD[LabeledPoint], sc:SparkContext, cols:Int)
  {
    //obtenemos los labels de la variable clase
    val labels2Int = data.map(_.label).distinct.collect.zipWithIndex.toMap
    val nLabels = labels2Int.size
    //metemos en cache la variable
    val bLabels2Int = sc.broadcast(labels2Int)
    
    //crea un map de los index labels y los cuenta, obteniendo el numero de apariciones
    //de cada distinct
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
     
     
     //mientras queden cutPoints candidatos..
     for (dimension <- 0 until cols)
     {
       val dataDim = sortedValues.filter(_._1._1 == dimension).map({case ((dim,value),hlabels) => (value,(hlabels, 0.floatValue()))}) 
       caimDiscretization(dataDim)
     }
     
  }
  
  def caimDiscretization(remainingCPs: RDD[(Float, (Array[Long], Float))])
  {
   /* INICIALIZACION DEL BUCLE
    * 
    * cutPoints = {maxValue} --> puntos de corte seleccionados
    * numCPs = 1 --> numero de cutPoints
    * remainingCPs --> posibles cutPoints, en cola para analizar
    * numRemainingCPs = length(remainingCPs) --> ...
    * GlobalBins = {minValue-maxValue} --> bins/particiones actuales
    * 
    * */
    //CAIM vencedor actual (inicializado a rango completo) y cutPoints escogidos
    val selectedCutPoints = ArrayBuffer[Float]()
    selectedCutPoints += remainingCPs.first._1 //minimo
    selectedCutPoints += remainingCPs.keys.max //maximo
    var classHistogram = remainingCPs.values.reduce((x,y) => ((for(i <- 0 until x._1.length) yield x._1(i) + y._1(i)).toArray,x._2))
    var fullRangeBin = ( (selectedCutPoints(0) , selectedCutPoints(1)) ,classHistogram._1)
    finalBins += fullRangeBin
    var GlobalCAIM = fullRangeBin._2
    
    /*variables temporales de cada iteracion*/
    var numRemainingCPs = remainingCPs.count()
    numRemainingCPs -= 2 //minimo y maximo extraidos antes
    var exit = false
    while(numRemainingCPs > 0 && !exit)
    {
      //Iterar sobre todos los cutPoints candidatos, obteniendo bins y caim
      actualTempBin = -1
      //TODO initialize tempBins
      remainingCPs.mapPartitions({ iter: Iterator[(Float, (Array[Long],Float))] => for (i <- iter) yield computeCAIM(i) }, true)
      
      //Coger el mejor CAIM y anadir ese punto a los cutPoints definitivos (eliminarlo de candidato)
      
      //actualizar bins y caims de bins
      numRemainingCPs - 1
    }
  }
  
  def computeCAIM(candidatePoint:(Float, (Array[Long],Float))): (Float, (Array[Long],Float)) =
  {
    if (candidatePoint._2._2 < 0) //Caso cutPoint ya definitivo
    {
      actualTempBin += 1
      //TODO crear nuevo Bin vacio en el tempBin entre este punto y palante
      //TODO crear zeroHistogram = (0,0,0,..) tamano nLabels de clase
      val zeroHistogram = new Array[Long](5)
      for (i <- 0 until zeroHistogram.length) zeroHistogram(i) = 0
      tempBins.insert(actualTempBin, (0.toFloat, zeroHistogram))
      candidatePoint
    }
    else
    {
      //Nuevo CAIM del Bin de la izquierda 
      var binHistogram = tempBins(actualTempBin)._2
      var newPointHistogram = candidatePoint._2._1
      var newBinHistogram = for(i <- 0 until binHistogram.length) yield binHistogram(i) + newPointHistogram(i)
      var newCaimBin = ( (newBinHistogram).max ^ 2 ) / newBinHistogram.sum.toFloat
      tempBins(actualTempBin) = (newCaimBin , newBinHistogram.toArray) //izquierda
      
      //Nuevo CAIM del Bin de la derecha
      binHistogram = tempBins(actualTempBin + 1)._2
      newPointHistogram = candidatePoint._2._1
      newBinHistogram = for(i <- 0 until binHistogram.length) yield binHistogram(i) - newPointHistogram(i)
      newCaimBin = ( (newBinHistogram).max ^ 2 ) / newBinHistogram.sum.toFloat
      tempBins(actualTempBin + 1) = (newCaimBin , newBinHistogram.toArray) //derecha
      
    }
    //new CAIM point
    var newCaimPoint = (for (i <- tempBins) yield i._1).sum
    (candidatePoint._1, (candidatePoint._2._1,newCaimPoint))
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
    println("done sortByKey in " + (System.currentTimeMillis() - start))
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