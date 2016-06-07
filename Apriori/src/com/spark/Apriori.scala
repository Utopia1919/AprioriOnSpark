package com.spark


import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.SortedSet
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Utopia on 2016/4/21.
 * args[0]  --minSupport
 * args[1]  --inputFile's path
 * args[2]  --outputFile's path and fileName without sign such as "F://something/Last/L" then your file will be "L1 , L2 , L3 ..."
 */
object Apriori {
  def main (args: Array[String]){

    val conf = new SparkConf()
    conf.setAppName("Apriori")
    //conf.setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(1))
    val startTime = System.currentTimeMillis()
    val db = lines.map(_.split(" ").map(each =>SortedSet(each.toInt)).reduce( _ ++ _)).cache()   //split with " "
    val minSupport = ( db.count() * args(0).toDouble ).toInt

    var Lk = lines.flatMap(_.split(" ")).map(word => (SortedSet(word.toInt), 1)).reduceByKey(_ + _).filter(_._2 >= minSupport).cache()   //收集K_1项集

    var Lk_last = Lk.map( kv => kv._1)
    var sign = 0
    while(Lk.count() != 0){
      sign = sign + 1
      Lk.saveAsObjectFile(args(2) + sign.toString)
      Lk_last = Lk.map( kv => kv._1)
      val L_k = Lk_last.collect()
      val Candidate_k = AprioriGen(L_k)
      val Breadcast_Ck = sc.broadcast(Candidate_k)
      Lk = db.flatMap{
        kv =>
          val temp = ArrayBuffer[(SortedSet[Int] , Int)]()
          Breadcast_Ck.value.foreach{
            k =>
              if(k.subsetOf(kv))
              {
                val s = (k,1)
                temp += s
              }
          }
          temp
      }.reduceByKey(_ + _).filter(_._2 >= minSupport).cache()
      Breadcast_Ck.unpersist()

    }
    if(sign == 0)
      println("minSupport is larger than 1-itemset")
    else
      Lk_last.collect().foreach(println(_))

    println(System.currentTimeMillis() - startTime)
  }

  // k frequent itemset to k + 1 candidate
  def AprioriGen(L_k: Array[SortedSet[Int]]): Array[(SortedSet[Int])] = {

    val Ck = ArrayBuffer[SortedSet[Int]]()
    L_k.foreach{
      l1 =>
        L_k.foreach{
          l2 =>
            if( l1.init == l2.init && l1.last < l2.last){
              val c = l1.init + l1.last + l2.last
              if(has_infrequent_subset(c,L_k)){
                Ck += c
              }
            }
        }
    }
    Ck.toArray
  }
  def has_infrequent_subset(candidate : SortedSet[Int] , L_k : Array[SortedSet[Int]]): Boolean ={
    candidate.subsets(candidate.size -1).foreach{
      s =>
        if(L_k.contains(s))
          return true
    }
    return false
  }
}
