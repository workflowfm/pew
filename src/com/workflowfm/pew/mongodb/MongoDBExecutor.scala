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
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.SingleObservable
import org.mongodb.scala.Observer
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import org.mongodb.scala.ReadConcern
import org.mongodb.scala.WriteConcern
import akka.dispatch.OnComplete

class MongoDBExecutor(client:MongoClient, db:String, collection:String, processes:Map[String,PiProcess],timeout:Duration=10.seconds)(override implicit val context: ExecutionContext = ExecutionContext.global) extends FutureExecutor {
  def this(client:MongoClient, db:String, collection:String, l:PiProcess*) = this(client,db,collection,PiProcess.mapOf(l :_*))  

  final val CAS_MAX_ATTEMPTS = 10
  final val CAS_WAIT_MS = 1
  
  val codecRegistry = fromRegistries(fromProviders(new PiCodecProvider(processes)),DEFAULT_CODEC_REGISTRY)
  val database: MongoDatabase = client.getDatabase(db).
    withCodecRegistry(codecRegistry).
    //withReadConcern(ReadConcern.LINEARIZABLE).
    withWriteConcern(WriteConcern.MAJORITY);
  val col:MongoCollection[PiInstance[ObjectId]] = database.getCollection(collection)
  
  val handler = new PromiseHandler[ObjectId]  
    
  def await[T](obs:Observable[T]) = Await.result(obs.toFuture,timeout)
  //def await[T](f:Future[T]) = Await.result(f,timeout)
  
  def call(p:PiProcess,args:PiObject*) = {
    val oid = new ObjectId
	  val inst = PiInstance(oid,p,args:_*)
	  val ret = handler.start(inst)
	  val ni = inst.reduce
    if (ni.completed) ni.result match {
		  case None => {
			  handler.failure(ni,ProcessExecutor.NoResultException(ni.id.toString()))
		  }
		  case Some(res) => {
			  handler.success(ni, res)
		  }
	  } else {
		  val (toCall,res) = ni.handleThreads(handleThread(ni.id))
      await(col.insertOne(res))
      val futureCalls = toCall flatMap (res.piFutureOf)
			(toCall zip futureCalls) map runThread(oid)
	  }
	  ret
  }
  
  
  //@tailrec
  final def postResult(id:ObjectId, ref:Int, res:PiObject, attempt:Int=0):Unit = {
		System.err.println("*** [" + id + "] Got result for thread " + ref + " : " + res)
    implicit val iid:ObjectId = id   
    
    val obs = client.startSession(ClientSessionOptions.builder.causallyConsistent(true).build()) safeThen { 
      case Seq(session:ClientSession) => { 
      System.err.println("*** [" + id + "] Session started: " + ref)
      col.find(session,equal("_id",id)) safeThen {
        case Seq() => {
          System.err.println("*** [" + id + "] CAS (pre): " + ref + " - Closing session")
          session.close()
          if (attempt < CAS_MAX_ATTEMPTS) Future.failed(CASException)
          else throw ProcessExecutor.NoSuchInstanceException(id.toString())
        }
        case Seq(i:PiInstance[ObjectId]) => {
        System.err.println("*** [" + id + "] Got PiInstance: " + i)
        val (toCall,resobs) = postResult(i,ref,res,col,session) 
        resobs safeThen { //.recover({ case ex => handler.failure(i,ex)})
          case Seq() => {
            System.err.println("*** [" + id + "] CAS (post): " + ref + " - Closing session")
            session.close()
            if (attempt < CAS_MAX_ATTEMPTS) Future.failed(CASException)
            else throw CASFailureException(id.toString(),ref)
          }  
          case Seq(_) => {
            System.err.println("*** [" + id + "] Closing session: " + ref)
            session.close()
            Future.successful(toCall)
          }
        }
      }}
    }}
    obs.onComplete({
      case Success(toCall) => { 
        System.err.println("*** [" + id + "] Success: " + ref)
        toCall map runThread(id)
      }
      case Failure(CASException) => System.err.println("*** [" + id + "] CAS Retry: " + ref + " - attempt: " + (attempt+1)); Thread.sleep(CAS_WAIT_MS); postResult(id,ref,res,attempt+1)
      case Failure(e) => System.err.println("*** [" + id + "] Failed: " + ref); handler.failure(id,e)
    })
  }
  
  def postResult(i:PiInstance[ObjectId], ref:Int, res:PiObject, col:MongoCollection[PiInstance[ObjectId]], session:ClientSession):(Seq[(Int,PiFuture)],Observable[_]) = {
    System.err.println("*** [" + i.id + "] Handling result for thread " + ref + " : " + res)
    val unique = i.called
	  val ni = i.postResult(ref, res).reduce
	  if (ni.completed) ni.result match {
		  case None => {   
		    // Delete first, then announce the result, so that we don't do anything (like close the pool) early
		    System.err.println("*** [" + i.id + "] Completed with no result!")
		    (Seq(),(col.findOneAndDelete(session,and(equal("_id",i.id),equal("calls",unique))) andThen { case _ => handler.failure(ni,ProcessExecutor.NoResultException(ni.id.toString()))}))
		  }
		  case Some(res) => {
		    System.err.println("*** [" + i.id + "] Completed with result!")
		    // Delete first, then announce the result, so that we don't do anything (like close the pool) early
		    (Seq(),(col.findOneAndDelete(session,and(equal("_id",i.id),equal("calls",unique))) andThen {case _ => handler.success(i,res)}))
		  }
		} else {
		  System.err.println("*** [" + i.id + "] Handling threads after: " + ref)
		  val (toCall,resi) = ni.handleThreads(handleThread(ni.id))
		  val futureCalls = toCall flatMap (resi.piFutureOf)
		  System.err.println("*** [" + i.id + "] Updating state after: " + ref)
			(toCall zip futureCalls,col.findOneAndReplace(session,and(equal("_id",i.id),equal("calls",unique)), resi))
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
      case Some(p:AtomicProcess) => true
      case Some(p:CompositeProcess) => { System.err.println("*** [" + id + "] Executor encountered composite process thread: " + name); false } // TODO this should never happen!
    }
  } }
  
  def runThread(id:ObjectId)(t:(Int,PiFuture)):Unit = {
    System.err.println("*** [" + id + "] Handling thread: " + t._1 + " (" + t._2.fun + ")")
    t match {
    case (ref,PiFuture(name, outChan, args)) => processes get name match {
      case None => {
        // This should never happen! We already checked!
        System.err.println("*** [" + id + "] ERROR *** Unable to find process: " + name + " even though we checked already")
      }
      case Some(p:AtomicProcess) => {
        p.run(args map (_.obj)).onComplete{ 
          case Success(res) => postResult(id,ref,res) 
          case Failure (ex) => handler.failure(id,ex)
        }
        System.err.println("*** [" + id + "] Called process: " + p.name + " ref:" + ref)
      }
      case Some(p:CompositeProcess) => {// This should never happen! We already checked! 
        System.err.println("*** [" + id + "] Executor encountered composite process thread: " + name)
      }
    }
  } }
  
  implicit class SafeObservable[T](obs: Observable[T])(implicit id:ObjectId) {
//    def safeThen[U](f: Seq[T] => U): Future[U] = {
//      val p = Promise[U]()
//      obs.toFuture().onComplete({
//        case Success(r:Seq[T]) => p.success(f(r))
//        case Failure(ex:Throwable) => p.failure(ex)
//      })
//      p.future
//    }
    def safeThen[U](f: Seq[T] => Future[U]): Future[U] = {
      val p = Promise[U]()
      obs.toFuture().onComplete({
        case Success(r:Seq[T]) => f(r).onComplete({
          case Success(s:U) => p.success(s)
          case Failure(e:Throwable) => p.failure(e)
        })
        case Failure(ex:Throwable) => p.failure(ex)
      })
      p.future
    }
  }

  final case object CASException extends Exception("CAS")
  final case class CASFailureException(val id:String, val ref:Int, private val cause: Throwable = None.orNull)
                    extends Exception("Compare-and-swap failed after " + CAS_MAX_ATTEMPTS + " attempts for id: " + id + " - call id: " + ref, cause)  
  override def execute(process:PiProcess,args:Seq[Any]):Future[Option[Any]] =
    call(process,args map PiObject.apply :_*)
   
}


    