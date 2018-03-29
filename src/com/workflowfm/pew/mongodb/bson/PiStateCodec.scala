package com.workflowfm.pew.mongodb.bson

import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import com.workflowfm.pew._
import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry
import scala.collection.mutable.Queue

/*
 * case class PiState(inputs:Map[Chan,Input], 
 *                    outputs:Map[Chan,Output], 
 *                    calls:List[PiFuture], 
 *                    threads:Map[Int,PiFuture], 
 *                    threadCtr:Int, 
 *                    freshCtr:Int, 
 *                    processes:Map[String,PiProcess], 
 *                    resources:ChanMap)
 */
class PiStateCodec {
  
}