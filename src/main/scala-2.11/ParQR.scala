package com.memantle.parqr
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel


object ParQR {
  def main(args: Array[String]) {

    var inputDir = args {0}
    var configFile = args {1}
    val parallel = args {2}.toInt
    val strategy = args{3}


    //set-up Spark Context
    val conf = new SparkConf()
      .setAppName("ParQR - Parallel Qualitative Reasoner")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    //generate the calculus
    val config = sc.textFile(configFile).collect
    val calculus=new Calculus(config(0),config(1),config.drop(2))


    //convert the input lines into tuples of the form (Int,BitSet,Int,Int)
    val inputRels = PreProcessFncs.convertInput(sc.textFile(inputDir),calculus)
    //get transpose of input network
    val inverseRels = PreProcessFncs.getTranspose(inputRels,calculus)
    //combine transpose with initial input
    val allInputRels = inputRels.union(inverseRels)



    //convert BitSet into Int for input dataset
    val setToIntlookUp = calculus.setToIntLookUp
    val universalRel = calculus.universalRel
    val consistentRelations = allInputRels.map(x=>(x._1,setToIntlookUp(x._2),x._3,x._4))


    //get a list of input relations and generate possible results of composition  based on this
    val possibleRels=PreProcessFncs.generatePossibleRelations(allInputRels,calculus)

    //using this list of relations pre-compute the composition table
    val compTbl = PreProcessFncs.preComputeCompTbl(possibleRels,calculus)

    //the same but pre-compute the intersection table
    val intersectTbl = PreProcessFncs.preComputeIntersectTbl(possibleRels, calculus)

    //now generate a size of relation look-up table
    val relSizeTbl = PreProcessFncs.preComputeSizeTbl(possibleRels, calculus)

    //put the look-up tables into broadcast variables
    val compTblLookUp = sc.broadcast(compTbl)
    val intersectTblLookUp = sc.broadcast(intersectTbl)
    val relSizeTblLookUp = sc.broadcast(relSizeTbl)




    //first pass of consistency
    var allTriples = if(strategy=="linear"){
      LinearFncs.consistency(consistentRelations, relSizeTblLookUp, intersectTblLookUp, parallel)
    }else{
      SmartFncs.consistency(consistentRelations, relSizeTblLookUp, intersectTblLookUp, parallel)
    }

    //put consistent input network into cache
    allTriples.persist(StorageLevel.MEMORY_ONLY_SER)


    var itr = 1
    var continue = true
    var currentCount = allTriples.count()

    while (continue) {
      //inference
      val newEdges = if (strategy == "linear") {
        LinearFncs.inference(allTriples, parallel, itr, compTblLookUp)
          .filter(rel => rel._2 != universalRel)
          .distinct()
          .persist(StorageLevel.MEMORY_ONLY_SER)
      }else{
        SmartFncs.inference(allTriples, parallel, itr, compTblLookUp)
          .filter(rel => rel._2 != universalRel)
          .distinct()
          .persist(StorageLevel.MEMORY_ONLY_SER)
      }

      //consistency
      allTriples = if(strategy=="linear"){
        LinearFncs.consistency(allTriples.union(newEdges),relSizeTblLookUp, intersectTblLookUp, parallel)
      }else{
        SmartFncs.consistency(allTriples.union(newEdges),relSizeTblLookUp, intersectTblLookUp, parallel)
      }
      allTriples.persist(StorageLevel.MEMORY_ONLY_SER)

      val newCount = allTriples.count
      if (newCount.equals(currentCount)) {
        continue = false
      }
      else {
        itr += 1
        currentCount = newCount
      }
    }

    if(allTriples.filter(x=>x._2==0).count()==0){
      println("\nconsistent")
    }

    sc.stop()
  }
}
