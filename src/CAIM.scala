import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.broadcast.Broadcast
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object CAIM {
  var numLabels = 0
  //Bins variables = ( (CutPointInit, CutPointEnd)  , (ClassHistogram, CAIM, binID) )
  val finalBins = ArrayBuffer[((Float, Float), (Array[Long], Double, Int))] ()
  var globalCaim = -Double.MaxValue
  var nTempBins = 2.0
  val selectedCutPoints = ArrayBuffer[Float]()
  var sc: SparkContext = null
  
  def discretizeData(data: RDD[LabeledPoint], sPc:SparkContext, cols:Int): ArrayBuffer[(Int,(Float,Float))] =
  //def discretizeData(data: RDD[LabeledPoint], sc:SparkContext, cols:Int)
  {
    sc = sPc
    //obtenemos los labels de la variable clase
    val labels2Int = data.map(_.label).distinct.collect.zipWithIndex.toMap
    numLabels = labels2Int.size
    //metemos en cache la variable
    val bLabels2Int = sc.broadcast(labels2Int)
    
    //crea un map de los index labels y los cuenta, obteniendo el numero de apariciones
    //de cada distinct
    val classDistrib = data.map(d => bLabels2Int.value(d.label)).countByValue()
    val bclassDistrib = sc.broadcast(classDistrib)
    println("numLabels: " + numLabels)
    println("bLabelsToInt: ")
    bLabels2Int.value.foreach(println(_))
    val featureValues =
        data.flatMap({
          case LabeledPoint(label, dv: DenseVector) =>
            val c = Array.fill[Long](numLabels)(0L)
            c(bLabels2Int.value(label)) = 1L
            for (i <- dv.values.indices) yield ((i, dv(i).toFloat), c)
          case LabeledPoint(label, sv: SparseVector) =>
            val c = Array.fill[Long](numLabels)(0L)
            c(bLabels2Int.value(label)) = 1L
            for (i <- sv.indices.indices) yield ((sv.indices(i), sv.values(i).toFloat), c)
        })
     println("featureValues first: " + featureValues.first._1 + ", " + featureValues.first._2)
     val sortedValues = getSortedDistinctValues(bclassDistrib, featureValues)     
     
     //Aplicar CAIM a cada dimension
     val bins = ArrayBuffer[(Int,(Float,Float))]()
     for (dimension <- 0 until cols)
     {
       val dataDim = sortedValues.filter(_._1._1 == dimension).map({case ((dim,value),hlabels) => (value,(hlabels, 0.0,0))}) 
       bins ++= caimDiscretization(dimension,dataDim)
     }
     bins
  }
  
  def caimDiscretization(dimension:Int, remainingCPs: RDD[(Float, (Array[Long], Double, Int))]): ArrayBuffer[(Int,(Float,Float))] =
  {
    //TESTING
    println("//--------------------------------------//")
    println("DIMENSION --> " + dimension)
    println("//--------------------------------------//")
    println
    //END TESTING
    
    var remCPs = remainingCPs
   /* INICIALIZACION DEL BUCLE
    * 
    * remainingCPs = inVariable --> posibles cutPoints, en cola para analizar
    * numRemainingCPs = length(remainingCPs) --> ...
    * selectedCutPoints = {minValue, maxValue} --> puntos de corte seleccionados
    * finalBins = {minValue-maxValue} --> bins/particiones actuales
    * nTempBins = 2 --> numero de temporal Bins de la iteracion actual
    * 
    * */
    selectedCutPoints.clear
    selectedCutPoints += remainingCPs.first._1 //minimo
    selectedCutPoints += remainingCPs.keys.max //maximo
    
    var classHistogram = remainingCPs.values.reduce((x,y) => ((for(i <- 0 until x._1.length) yield x._1(i) + y._1(i)).toArray, 0, 0))
    //TODO check if initialization of globalCaim is correct
    //globalCaim = (classHistogram._1.max * classHistogram._1.max) / classHistogram._1.sum.toDouble
    globalCaim = -Double.MaxValue
    var fullRangeBin = ( (selectedCutPoints(0) , selectedCutPoints(1)) ,(classHistogram._1, globalCaim, 0))
    finalBins.clear()
    finalBins += fullRangeBin
    nTempBins = 2.0
    //TODO min and max --> set CAIM = -1
    
    /*variables temporales de cada iteracion*/
    var numRemainingCPs = remainingCPs.count()
    numRemainingCPs -= 2 //minimo y maximo extraidos antes
    var exit = false
    
    while(numRemainingCPs > 0 && !exit)
    {
      //TESTING
      println("-----------------------")
      println("NumRemainingCPs (iteracion numero..) --> " + numRemainingCPs)
      println("-----------------------")
      println("DATOS INICIALES: ")
      print("Selected cutPoints --> ")
      for(i <- selectedCutPoints) print(i + ", ")
      println
      print("final Bins --> ")
      for(i <- finalBins) print("(" + i._1._1 + ", " + i._1._2 + "):" + i._2._2 + "; ")
      println
      println("global Caim --> " + globalCaim)
      println("num temp Bins --> " + nTempBins)
      println
      println("CALCULO DE NUEVOS CAIMS INICIADO...")
      //END TESTING
      
      //generate new CAIM database
      
      val bRemCPs = sc.broadcast(remCPs)
      remCPs = remCPs.mapPartitions({ iter: Iterator[(Float, (Array[Long],Double, Int))] => for (i <- iter) yield computeCAIM(i, bRemCPs.value.filter(item => item._2._3 == i._2._3)) }, true)
      remCPs.persist
      //TESTING
      println("CAIMS OBTENIDOS: ")
      var acum = 0
      remCPs.collect.foreach({item =>
        acum += item._2._1.sum.toInt
        println("Punto " + item._1 + ", NumPoints: " + item._2._1.sum + ", Acumulator: " + acum + ", CAIM: " + item._2._2)
      })
      //END TESTING
      //Coger el mejor CAIM y anadir ese punto a los cutPoints definitivos (eliminarlo de candidato, haciendo su CAIM = -1)
      val bestCandidate = remCPs.max()(new Ordering[Tuple2[Float, Tuple3[Array[Long],Double, Int]]]() {
        override def compare(x: (Float, (Array[Long],Double, Int)), y: (Float, (Array[Long],Double, Int))): Int = 
        Ordering[Double].compare(x._2._2, y._2._2)
      })
      
      //TESTING
      println("CALCULO DE NUEVOS CAIMS FINALIZADO...")
      println
      println("Mejor candidato --> " + bestCandidate._1)
      println("Caim de mejor candidato:" + bestCandidate._2._2 + " |vs| Caim global actual:" + globalCaim)
      println("nTempBins:" + nTempBins + " |vs| numLabels:" + numLabels)
      println
      println("ENTRANDO A INTRODUCIR NUEVO PUNTO...")
      //END TESTING
      
      if(bestCandidate._2._2 > globalCaim || nTempBins < numLabels)
      {
        selectedCutPoints += bestCandidate._1
        
        //finalBins: ArrayBuffer ( (CutPointInit, CutPointEnd)  , (ClassHistogram, CAIM, binID) )
        var i = 0
        var found = false
        while (i < finalBins.length && !found)
        {
          if (bestCandidate._1 < finalBins(i)._1._2)
          {
            found = true
            val dataBin = bRemCPs.value.filter(item => item._2._3 == i)
            //crear bin y recalcular caim
            dataBin.cache
            val valuesLeft = dataBin.filter(_._1 <= bestCandidate._1).values.reduce((x,y) => ((for(i <- 0 until numLabels) yield x._1(i) + y._1(i)).toArray, 0, 0))
            val valuesRight = dataBin.filter(_._1 > bestCandidate._1).values.reduce((x,y) => ((for(i <- 0 until numLabels) yield x._1(i) + y._1(i)).toArray, 0, 0))
            val caimLeft = (valuesLeft._1.max * valuesLeft._1.max) / (valuesLeft._1.sum.toDouble)
            val caimRight = (valuesRight._1.max * valuesLeft._1.max) / (valuesRight._1.sum.toDouble)
            //TESTING
            println("LEFT --> Valor maximo: " + valuesLeft._1.max + ", suma de puntos: " + valuesLeft._1.sum.toDouble + ", CAIM: " + caimLeft)
            println("RIGHT --> Valor maximo: " + valuesRight._1.max + ", suma de puntos: " + valuesRight._1.sum.toDouble + ", CAIM: " + caimRight)
            //END TESTING
            val binLeft = ((finalBins(i)._1._1 , bestCandidate._1), (valuesLeft._1 , caimLeft , i ))
            val binRight = ((bestCandidate._1 , finalBins(i)._1._2), (valuesRight._1 , caimRight , i + 1))
            finalBins(i) = binRight
            finalBins.insert(i, binLeft)
            globalCaim = bestCandidate._2._2
            
            //TESTING
            var sumBins = 0.0
            for(item <- finalBins){
              sumBins += item._2._2
              println("Bin (" + item._1._1 + "," + + item._1._2 + ") CAIM--> " + item._2._2)
            }
            println("SE INTRODUCE --> Caim definido: " + globalCaim + ", Caim recalculado: " + sumBins/nTempBins)
            //END TESTING
          }
        }
        if(!found) println("Bin no encontrado!")
        nTempBins += 1.0
        numRemainingCPs -= 1
      }
      else
        exit = true
      //TESTING
      println("FIN DE ANADIDO DE PUNTO...")
      println
      println("DATOS FINALES ITERACION:")
      print("Selected cutPoints --> ")
      for(i <- selectedCutPoints) print(i + ", ")
      println
      print("final Bins --> ")
      for(i <- finalBins) print("(" + i._1._1 + ", " + i._1._2 + "):" + i._2._2 + "; ")
      println
      println("global Caim --> " + globalCaim)
      println("num temp Bins --> " + nTempBins)
      println
      //END TESTING
    }
    //TESTING
    print("DATOS FINALES:")
    print("Selected cutPoints --> ")
    for(i <- selectedCutPoints) print(i + ", ")
    println
    print("final Bins --> ")
    for(i <- finalBins) print("(" + i._1._1 + ", " + i._1._2 + "); ")
    println
    println("END DIMENSION " + dimension)
    println
    //END TESTING
    
    //add dimension ID to bins
    val result = ArrayBuffer[(Int,(Float,Float))]()
    result ++= (for (bin <- finalBins) yield (dimension, bin._1))
    result
  }
  
  //candidatePoint = (value, (ClassHistogram, CAIM))
  def computeCAIM(candidatePoint:(Float, (Array[Long],Double, Int)), dataBin: RDD[(Float, (Array[Long],Double, Int))]): (Float, (Array[Long],Double, Int)) =
  {
    //TODO arreglar esto, pequeño fix que suple el no poder modificar CAIM a -1 --> if(candidatePoint._2._2 < 0) 
    if (selectedCutPoints.exists(_ == candidatePoint._1))
    {
      //candidatePoint
      (candidatePoint._1,(candidatePoint._2._1, -1, candidatePoint._2._3))
    }
    else //Caso 2 = cutPoint es candidato
    {
      //filtrar puntos de interes, dividir datos en dos, y obtener suma de histogramas
      dataBin.cache
      val valuesLeft = dataBin.filter(_._1 <= candidatePoint._1).values.reduce((x,y) => ((for(i <- 0 until numLabels) yield x._1(i) + y._1(i)).toArray, 0, 0))
      val valuesRight = dataBin.filter(_._1 > candidatePoint._1).values.reduce((x,y) => ((for(i <- 0 until numLabels) yield x._1(i) + y._1(i)).toArray, 0, 0))
      
      //calcular CAIM
      var caimBins = ( (valuesLeft._1.max * valuesLeft._1.max) / valuesLeft._1.sum.toDouble) + ((valuesRight._1.max * valuesRight._1.max) / valuesRight._1.sum.toDouble )
      for(item <- finalBins if (item._2._3 != candidatePoint._2._3)) caimBins += item._2._2
      
      //return result
      (candidatePoint._1,(candidatePoint._2._1, caimBins/nTempBins, candidatePoint._2._3))
    }
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