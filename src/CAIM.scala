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
  //Bins variables = ( (CutPointInit, CutPointEnd)  , (ClassHistogram, CAIM) )
  val finalBins = ArrayBuffer[((Float, Float), Double)] ()
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
    println("Num Labels: " + numLabels)
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
     val sortedValues = getSortedDistinctValues(bclassDistrib, featureValues)     
     
     //Aplicar CAIM a cada dimension
     val bins = ArrayBuffer[(Int,(Float,Float))]()
     for (dimension <- 0 until cols)
     {
       val dataDim = sortedValues.filter(_._1._1 == dimension).map({case ((dim,value),hlabels) => (value,(hlabels, 0.0))}) 
       bins ++= caimDiscretization(dimension,dataDim)
     }
     bins
  }
  
  def caimDiscretization(dimension:Int, remainingCPs: RDD[(Float, (Array[Long], Double))]): ArrayBuffer[(Int,(Float,Float))] =
  {
    //TESTING
    println
    println("//--------------------------------------//")
    println("//------------ DIMENSION " + dimension + " -------------//") 
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
    
    var classHistogram = remainingCPs.values.reduce((x,y) => ((for(i <- 0 until x._1.length) yield x._1(i) + y._1(i)).toArray, 0))
    globalCaim = -Double.MaxValue
    //min cutPoint esta como el minimo valor de float para que entre dentro de los rangos al escanear --> dataBin = (min < x <= max)
    var fullRangeBin = ( (Float.MinValue , selectedCutPoints(1)), globalCaim)  
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
      for(i <- finalBins) print("(" + i._1._1 + ", " + i._1._2 + "):" + i._2 + "; ")
      println
      println("global Caim --> " + globalCaim)
      println("num temp Bins --> " + nTempBins)
      println
      println("CALCULO DE NUEVOS CAIMS INICIADO...")
      //END TESTING
      
      //generate new CAIM database
      
      val bRemCPs = sc.broadcast(remCPs)
      remCPs = remCPs.mapPartitions({ iter: Iterator[(Float, (Array[Long],Double))] => for (i <- iter) yield computeCAIM(i, bRemCPs) }, true)
      remCPs.persist
      //TESTING
      println("CAIMS OBTENIDOS: ")
      remCPs.collect.foreach({item =>
        println("Punto " + item._1 + ", CAIM: " + item._2._2)
      })
      //END TESTING
      //Coger el mejor CAIM y anadir ese punto a los cutPoints definitivos (eliminarlo de candidato, haciendo su CAIM = -1)
      val bestCandidate = remCPs.max()(new Ordering[Tuple2[Float, Tuple2[Array[Long],Double]]]() {
        override def compare(x: (Float, (Array[Long],Double)), y: (Float, (Array[Long],Double))): Int = 
        Ordering[Double].compare(x._2._2, y._2._2)
      })
      /*
       * otra forma de hacerlo seria bestCandidate = mapPartitions.max
       * sacar el filter de la funcion y ponerlo dentro de mappartitions
       * broadcast selectedCutpoints y finalBins
       * el mapPartitions debe generar la lista de (point:float, CAIM:double)
       * */
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
            val actualBin = finalBins(i)
            val dataBin = bRemCPs.value.filter(item => (actualBin._1._1 < item._1) && (actualBin._1._2 >= item._1))
            //crear bin y recalcular caim
            dataBin.cache
            //TODO podemos ahorrarnos un filter aqui, qutando el filter del bin y dejando solo valuesLeft filter y valuesRight filter.
            val valuesLeft = dataBin.filter(_._1 <= bestCandidate._1).values.reduce((x,y) => ((for(i <- 0 until numLabels) yield x._1(i) + y._1(i)).toArray, 0))
            val valuesRight = dataBin.filter(_._1 > bestCandidate._1).values.reduce((x,y) => ((for(i <- 0 until numLabels) yield x._1(i) + y._1(i)).toArray, 0))
            val caimLeft = (valuesLeft._1.max * valuesLeft._1.max) / (valuesLeft._1.sum.toDouble)
            val caimRight = (valuesRight._1.max * valuesRight._1.max) / (valuesRight._1.sum.toDouble)
            /*TESTING
            println("LEFT --> Valor maximo: " + valuesLeft._1.max + ", suma de puntos: " + valuesLeft._1.sum.toDouble + ", CAIM: " + caimLeft)
            println("RIGHT --> Valor maximo: " + valuesRight._1.max + ", suma de puntos: " + valuesRight._1.sum.toDouble + ", CAIM: " + caimRight)
            //END TESTING*/
            val binLeft = ((finalBins(i)._1._1 , bestCandidate._1), caimLeft )
            val binRight = ((bestCandidate._1 , finalBins(i)._1._2), caimRight )
            finalBins(i) = binRight
            finalBins.insert(i, binLeft)
            globalCaim = bestCandidate._2._2
            
            //TESTING
            println("Bins nuevos")
            var sumBins = 0.0
            for(item <- finalBins){
              sumBins += item._2
              println("Bin (" + item._1._1 + "," + + item._1._2 + "), CAIM--> " + item._2)
            }
            println("SE INTRODUCE --> Caim definido durante mapPartitions: " + globalCaim + ", Caim recalculado ahora: " + sumBins/nTempBins)
            //END TESTING
          }
          i += 1
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
      for(i <- finalBins) print("(" + i._1._1 + ", " + i._1._2 + "):" + i._2 + "; ")
      println
      println("global Caim --> " + globalCaim)
      println("num temp Bins --> " + nTempBins)
      println
      //END TESTING
    }
    
    //corregimos el ajuste del primer bin, poniendo bien su minValue
    finalBins(0) = ( (selectedCutPoints(0), finalBins(0)._1._2), 0 )
    //add dimension ID to bins
    val result = ArrayBuffer[(Int,(Float,Float))]()
    result ++= (for (bin <- finalBins) yield (dimension, bin._1))
    //TESTING
    print("DATOS FINALES:")
    print("Selected cutPoints --> ")
    for(i <- selectedCutPoints) print(i + ", ")
    println
    print("Result Bins --> ")
    for(i <- finalBins) print("(" + i._1._1 + ", " + i._1._2 + "); ")
    println
    println("//--------------------------------------//")
    println("//---------- END DIMENSION " + dimension + " -----------//") 
    println("//--------------------------------------//")
    println
    //END TESTING
    result
  }
  
  //candidatePoint = (value, (ClassHistogram, CAIM))
  def computeCAIM(candidatePoint:(Float, (Array[Long],Double)), dataBc: Broadcast[RDD[(Float, (Array[Long],Double))]]): (Float, (Array[Long],Double)) =
  {
    //TODO arreglar esto, pequeño fix que suple el no poder modificar CAIM a -1 --> if(candidatePoint._2._2 < 0) 
    if (selectedCutPoints.exists(_ == candidatePoint._1))
    {
      //candidatePoint
      (candidatePoint._1,(candidatePoint._2._1, -1))
    }
    else //Caso 2 = cutPoint es candidato
    {
      //filtrar puntos de interes, dividir datos en dos, y obtener suma de histogramas
      //TODO podemos ahorrarnos un filter aqui, qutando el filter del bin y dejando solo valuesLeft filter y valuesRight filter. 
      val actualBin = finalBins.filter(bin => (bin._1._1 < candidatePoint._1) && (bin._1._2 >= candidatePoint._1))(0)
      val dataBin = dataBc.value.filter(item => (actualBin._1._1 < item._1) && (actualBin._1._2 >= item._1))
      dataBin.cache
      val valuesLeft = dataBin.filter(_._1 <= candidatePoint._1).values.reduce((x,y) => ((for(i <- 0 until numLabels) yield x._1(i) + y._1(i)).toArray, 0))
      val valuesRight = dataBin.filter(_._1 > candidatePoint._1).values.reduce((x,y) => ((for(i <- 0 until numLabels) yield x._1(i) + y._1(i)).toArray, 0))
      
      //calcular CAIM
      var caimBins = ( (valuesLeft._1.max * valuesLeft._1.max) / valuesLeft._1.sum.toDouble) + ((valuesRight._1.max * valuesRight._1.max) / valuesRight._1.sum.toDouble )
      for(item <- finalBins if (item._1._1 != actualBin._1._1)) caimBins += item._2
      
      //return result
      (candidatePoint._1,(candidatePoint._2._1, caimBins/nTempBins))
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