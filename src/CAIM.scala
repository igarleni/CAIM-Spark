import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.broadcast.Broadcast
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object CAIM {
  var tmpCaimNumerator = 0.0 //para la acumulacion de estadisticas
  var finalBins = ArrayBuffer[((Float, Float), Float)] ()
  
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
    selectedCutPoints += remainingCPs.first._1
    selectedCutPoints += remainingCPs.collect().last._1
    var classHistogram = remainingCPs.values.reduce((x,y) => ((for(i <- 0 until x._1.length) yield x._1(i) + y._1(i)).toArray,x._2))
    var fullRangeBin = ( (selectedCutPoints(0) , selectedCutPoints(1)) ,classHistogram._1.max.toFloat)
    finalBins += fullRangeBin
    var GlobalCAIM = fullRangeBin._2
    
    /*variables temporales de cada iteracion*/
    var numRemainingCPs = remainingCPs.count()
    numRemainingCPs -= 2 //minimo y maximo extraidos antes
    var exit = false
    while(numRemainingCPs > 0 && !exit)
    {
      //Iterar sobre todos los cutPoints candidatos, obteniendo bins y caim
      remainingCPs.mapPartitions({ iter: Iterator[(Float, (Array[Long]))] => for (i <- iter) yield i }, true)
      
      //Coger el mejor CAIM y anadir ese punto a los cutPoints definitivos (eliminarlo de candidato)
      //actualizar bins y caims de bins
      numRemainingCPs - 1
    }
  }
  
  def computeCAIM(candidatePoint:(Float, Array[Long]))
  {
    
    var tmpCaim = tmpCaimNumerator
    //calcular CAIM fijandose en  finalBins para reutilizar CAIM bins y no tener que recalcularlo
    
    //USAR FOLD(tmpCaim) ?????????
    
    tmpCaimNumerator = tmpCaim
    tmpCaim = tmpCaim / (finalBins.length + 1)
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