package com.workflowfm.pew.mongodb

import com.workflowfm.pew._
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

class MongoDBExecutor(client:MongoClient, db:String, collection:String, processes:Map[String,PiProcess],timeout:Duration=10.seconds)(override implicit val context: ExecutionContext = ExecutionContext.global) extends ProcessExecutor {
  //def this(store:PiInstanceStore[Int],l:PiProcess*) = this(store,PiProcess.mapOf(l :_*))
  //def this(l:PiProcess*) = this(SimpleInstanceStore(),PiProcess.mapOf(l :_*))
  
  var promises:Map[ObjectId,Promise[Option[Any]]] = Map()
  
  val codecRegistry = fromRegistries(fromProviders(new PiCodecProvider(processes)),DEFAULT_CODEC_REGISTRY)
  
  type ProgFun[T] = (PiInstance[ObjectId],ClientSession)=>Observable[T]
  
  def call(p:PiProcess,args:PiObject*) = {
    val oid = new ObjectId
	  val inst = PiInstance(oid,p,args:_*)
	  val promise = Promise[Option[Any]]()
	  System.err.println(" === INITIAL STATE " + oid + " === \n" + inst.state + "\n === === === === === === === ===")
	  promises += (oid -> promise)
	  run(inst)
	  promise.future
  }
  
  def await[T](obs:Observable[T]) = Await.result(obs.toFuture,timeout)
  
  def map[T](oid:ObjectId, f:ProgFun[T]):Unit = {
   val database: MongoDatabase = client.getDatabase(db).withCodecRegistry(codecRegistry);
   val col:MongoCollection[PiInstance[ObjectId]] = database.getCollection(collection)
    
   val sessionQ = await(client.startSession(ClientSessionOptions.builder.causallyConsistent(true).build()))
   // TODO Throw exception if empty
   val session = sessionQ.head
   val obs = col.find(session,Filters.equal("_id",oid)).flatMap({ i => f(i,session)})
   await(obs)
   session.close()
  }
  
  def clean(id:ObjectId, session:ClientSession) = {
    // TODO delete from MongoDB // store = store.del(id)
    promises = promises - id
  }
  
  def success(i:PiInstance[ObjectId], res:Any, session:ClientSession) = {
    System.err.println(" === FINAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
    promises.get(i.id) match {
      case Some(p) => p.success(Some(res))
      case None => Unit
    }
    clean(i.id, session:ClientSession)
  }
  
  def failure(inst:PiInstance[ObjectId], session:ClientSession) = {
	  System.err.println(" === Failed to obtain output " + inst.id + " ! ===")
	  System.err.println(" === FINAL STATE " + inst.id + " === \n" + inst.state + "\n === === === === === === === ===")
    promises.get(inst.id) match {
      case Some(p) => p.success(None)
      case None => Unit
    }
	  clean(inst.id, session:ClientSession)
  }
  
  def update(i:PiInstance[ObjectId], session:ClientSession) = {
    
  }
  
  final def run(i:PiInstance[ObjectId], session:ClientSession):Option[PiInstance[ObjectId]] = {
	  val ni = i.reduce
	  if (ni.completed) ni.result match {
		  case None => failure(ni,session); None
		  case Some(res) => success(i,res,session); None        
		} else {
			Some(ni.handleThreads(handleThread(ni.id)))
		}
  }
 
  def handleThread(id:ObjectId)(ref:Int,f:PiFuture):Boolean = f match {
    case PiFuture(name, outChan, args) => processes get name match {
      case None => {
        System.err.println("*** ERROR *** Unable to find process: " + name)
        false
      }
      case Some(p:AtomicProcess) => {
        p.run(args map (_.obj)).onSuccess{ case res => postResult(id,ref,res)}
        System.err.println("*** Called process: " + p.name + " id:" + id + " ref:" + ref)
        true
      }
      case Some(p:CompositeProcess) => { System.err.println("*** Executor encountered composite process thread: " + name); false } // TODO this should never happen!
    }
  }
    
  def postResult(id:ObjectId,ref:Int, res:PiObject):Unit = store.synchronized {
    System.err.println("*** Received result for ID:" + id + " Thread:" + ref + " : " + res)
    store.get(id) match {
      case None => System.err.println("*** No running instance! ***")
      case Some(i) => 
        if (i.id != id) System.err.println("*** Different instance ID encountered: " + i.id)
        else {
          run(i.postResult(ref, res))
        }
    }
  }
 
  override def execute(process:PiProcess,args:Seq[Any]):Future[Option[Any]] =
    call(process,args map PiObject.apply :_*)
}