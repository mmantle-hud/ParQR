package com.memantle.parqr

import org.apache.spark.{SparkContext, HashPartitioner}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.HashMap

object SmartFncs {

  //checks consistency of the network
  def consistency(relations:RDD[(Int,Int,Int,Int)],
                  relSizeTblLookUp: Broadcast[HashMap[Int,Int]],
                  intersectTblLookUp: Broadcast[HashMap[Int,HashMap[Int,Int]]],
                   parallel:Int)
  :RDD[(Int,Int,Int,Int)]=
    {
      relations
        .map(rel => (rel._1+"#"+rel._3,rel))
        .reduceByKey((a,b)=>
        {
          val sizeA=relSizeTblLookUp.value(a._2)
          val sizeB=relSizeTblLookUp.value(b._2)
          val iSect = intersectTblLookUp.value(a._2)(b._2)
          val sizeiSect=relSizeTblLookUp.value(iSect)

          //empty intersection
          if(iSect==0){
            println("inconsistent")
          }
          val pathLen = if(a._4 == b._4){ //they are the same it doesn't matter which one we use
            a._4
          }else if(a._4>b._4 && sizeB>sizeiSect){ //the new relation is smaller, so replace
            a._4
          }else if(b._4>a._4 && sizeA>sizeiSect){ //the new relation is smaller, so replace
            b._4
          }else{
            Math.min(a._4,b._4)//the new relation is the same size or bigger, so take the min
          }
          (a._1, iSect, a._3, pathLen)
        }).map(x=>x._2).repartition(parallel)
  }

  //infers new relations
  def inference(rels:RDD[(Int, Int, Int, Int)], parallel:Int, itr:Int, compTblLookUp: Broadcast[HashMap[Int, HashMap[Int,Int]]])
  :RDD[(Int, Int, Int, Int)] = {
    val len = Math.pow(2,(itr-1))
    val rhsRels = rels.filter((relation) => relation._4==len)
      .keyBy(_._3)
      .partitionBy(new HashPartitioner(parallel))
      .persist(StorageLevel.MEMORY_ONLY_SER)

    val lhsRels = rels
      .filter((relation) => relation._4 <=len)
      .keyBy(_._1)

    val joinedTriples = rhsRels.join(lhsRels)
      .filter(pairOfRels=>pairOfRels._2._1._1!=pairOfRels._2._2._3)// no joins to self

    joinedTriples.map(joinPair=>{
      val triple1=joinPair._2._1
      val triple2=joinPair._2._2
      val inferredRel = compTblLookUp.value(triple1._2)(triple2._2)
      (triple1._1, inferredRel, triple2._3, (triple1._4+triple2._4))
    })

  }

}
