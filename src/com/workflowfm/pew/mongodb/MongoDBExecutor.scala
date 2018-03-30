package com.workflowfm.pew.mongodb

import com.workflowfm.pew._
import com.workflowfm.pew.execution._
import com.workflowfm.pew.mongodb.bson.PiCodecProvider

import scala.concurrent._
import scala.concurrent.duration._
import scala.annotation.tailrec

import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry, CodecRegistries}
import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders, fromCodecs}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY

import com.mongodb.session.ClientSession
import org.mongodb.scala.MongoClient
import org.mongodb.scala.Observable
import org.mongodb.scala.ClientSessionOptions
import org.mongodb.scala.model.Filters
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.SingleObservable

class MongoDBExecutor(client:MongoClient, db:String, collection:String, processes:Map[String,PiProcess],timeout:Duration=10.seconds)(override implicit val context: ExecutionContext = ExecutionContext.global) extends FutureExecutor {
  def this(client:MongoClient, db:String, collection:String, l:PiProcess*) = this(client,db,collection,PiProcess.mapOf(l :_*))  
   
  val codecRegistry = fromRegistries(fromProviders(new PiCodecProvider(processes)),DEFAULT_CODEC_REGISTRY)
  
  val handler = new PromiseHandler[ObjectId]  
    
  def await[T](obs:Observable[T]) = Await.result(obs.toFuture,timeout)
  
  def call(p:PiProcess,args:PiObject*) = {
    val oid = new ObjectId
	  val inst = PiInstance(oid,p,args:_*)
	  val ret = handler.start(inst)
	  val ni = inst.reduce
    if (ni.completed) ni.result match {
		  case None => {
			  handler.failure(ni)
		  }
		  case Some(res) => {
			  handler.success(ni, res)
		  }
	  } else {
		  val res = ni.handleThreads(handleThread(ni.id))
		  val database: MongoDatabase = client.getDatabase(db).withCodecRegistry(codecRegistry);
      val col:MongoCollection[PiInstance[ObjectId]] = database.getCollection(collection)
      await(col.insertOne(res))
	  }
	  ret
  }
  
  
  
  final def postResult(id:ObjectId, ref:Int, res:PiObject):Unit = {
		val database: MongoDatabase = client.getDatabase(db).withCodecRegistry(codecRegistry);
    val col:MongoCollection[PiInstance[ObjectId]] = database.getCollection(collection)
   
    val sessionQ = client.startSession(ClientSessionOptions.builder.causallyConsistent(true).build())
    sessionQ.head().map { session => {
      val obs = col.find(session,Filters.equal("_id",id)).subscribe({ i:PiInstance[ObjectId] => await(
          postResult(i,ref,res,col,session).recover({ case ex => handler.failure(i,Some(ex))})
          )})
    // TODO handle more exceptions!
      session.close()
    }}
  }
  
  def postResult(i:PiInstance[ObjectId], ref:Int, res:PiObject, col:MongoCollection[PiInstance[ObjectId]], session:ClientSession):Observable[_] = {
    System.err.println("*** [" + i.id + "] Received result for thread " + ref + " : " + res)
	  val ni = i.postResult(ref, res).reduce
	  if (ni.completed) ni.result match {
		  case None => {
		    handler.failure(ni)
		    col.deleteOne(session,Filters.equal("_id",i.id))
		  }
		  case Some(res) => {
		    handler.success(i,res)
		    col.deleteOne(session,Filters.equal("_id",i.id))
		  }
		} else {
			col.replaceOne(session,Filters.equal("_id",i.id), ni.handleThreads(handleThread(ni.id)))
		}
  }
 
  def handleThread(id:ObjectId)(ref:Int,f:PiFuture):Boolean = {
     System.err.println("*** [" + id + "] Handling thread: " + ref + " (" + f.fun + ")")
    f match {
    case PiFuture(name, outChan, args) => processes get name match {
      case None => {
        System.err.println("*** [" + id + "] ERROR *** Unable to find process: " + name)
        false
      }
      case Some(p:AtomicProcess) => {
        p.run(args map (_.obj)).onSuccess{ case res => postResult(id,ref,res) }
        System.err.println("*** [" + id + "] Called process: " + p.name + " ref:" + ref)
        true
      }
      case Some(p:CompositeProcess) => { System.err.println("*** [" + id + "] Executor encountered composite process thread: " + name); false } // TODO this should never happen!
    }
  } }
 
  override def execute(process:PiProcess,args:Seq[Any]):Future[Option[Any]] =
    call(process,args map PiObject.apply :_*)
   
}


    