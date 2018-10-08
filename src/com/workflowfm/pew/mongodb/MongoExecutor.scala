package com.workflowfm.pew.mongodb

import com.workflowfm.pew._
import com.workflowfm.pew.execution._
import com.workflowfm.pew.mongodb.bson.PiCodecProvider

import scala.concurrent._
import scala.concurrent.duration._
import scala.annotation.tailrec
import scala.util.Try
import scala.util.Success
import scala.util.Failure

import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry, CodecRegistries}
import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders, fromCodecs}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY

import org.mongodb.scala.MongoClient
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.MongoCollection
import com.mongodb.session.ClientSession
import org.mongodb.scala.ClientSessionOptions
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.Observable
import org.mongodb.scala.SingleObservable
import org.mongodb.scala.Observer
import org.mongodb.scala.ReadConcern
import org.mongodb.scala.WriteConcern

class MongoExecutor(client:MongoClient, db:String, collection:String, processes:PiProcessStore, timeout:Duration=10.seconds)(implicit val context: ExecutionContext = ExecutionContext.global) extends ProcessExecutor[ObjectId] with SimplePiObservable[ObjectId] { 

  final val CAS_MAX_ATTEMPTS = 10
  final val CAS_WAIT_MS = 1
  
  val codecRegistry = fromRegistries(fromProviders(new PiCodecProvider(processes)),DEFAULT_CODEC_REGISTRY)
  val database: MongoDatabase = client.getDatabase(db).
    withCodecRegistry(codecRegistry).
    //withReadConcern(ReadConcern.LINEARIZABLE).
    withWriteConcern(WriteConcern.MAJORITY);
  val col:MongoCollection[PiInstance[ObjectId]] = database.getCollection(collection)

  
  override def call(p:PiProcess,args:Seq[PiObject]):Future[ObjectId] = {
    val oid = new ObjectId
	  val inst = PiInstance(oid,p,args:_*)
	  val ni = inst.reduce
    if (ni.completed) ni.result match {
		  case None => {
			  publish(PiEventFailure(ni,ProcessExecutor.NoResultException(ni.id.toString())))
		  }
		  case Some(res) => {
			  publish(PiEventResult(ni, res))
		  }
	  } else try {
		  val (toCall,res) = ni.handleThreads(handleThread(ni))
      col.insertOne(res).toFuture().onComplete({
        case Success(_) => {
          val futureCalls = toCall flatMap (res.piFutureOf)
  			  try {
  			    (toCall zip futureCalls) map runThread(res)
  			  } catch {
  			    case (e:Exception) => promise.failure(e)
  			  }
        }
        case Failure(e) => promise.failure(e)
		  })
	  } catch {
	    case (e:Exception) => {
	      publish(PiEventFailure(ni,e))
      }
    }
	 Future.successful(oid)
  }
  
  final def postResult(id:ObjectId, ref:Int, res:PiObject):Future[Unit] = {
		val promise = Promise[Unit]()
		postResult(id,ref,res,0,promise)
    promise.future
  }
  
  
  
  final def postResult(id:ObjectId, ref:Int, res:PiObject, attempt:Int, promise:Promise[Unit]):Unit = try {
		System.err.println("*** [" + id + "] Got result for thread " + ref + " : " + res)
    implicit val iid:ObjectId = id   
     
    val obs = client.startSession(ClientSessionOptions.builder.causallyConsistent(true).build()) safeThen { 
      case Seq(session:ClientSession) => { 
      System.err.println("*** [" + id + "] Session started: " + ref)
      col.find(session,equal("_id",id)) safeThen {
        case Seq() => Future {
          System.err.println("*** [" + id + "] CAS (pre): " + ref + " - Closing session")
          session.close()
          if (attempt < CAS_MAX_ATTEMPTS) throw CASException
          else throw ProcessExecutor.NoSuchInstanceException(id.toString())
        }
        case Seq(i:PiInstance[ObjectId]) => {
          System.err.println("*** [" + id + "] Got PiInstance: " + i)
          val (toCall,resobs) = postResult(i,ref,res,col,session)      
          resobs safeThen {
            case Seq() => Future {
              System.err.println("*** [" + id + "] CAS (post): " + ref + " - Closing session")
              session.close()
              if (attempt < CAS_MAX_ATTEMPTS) throw CASException
              else throw CASFailureException(id.toString(),ref)
            }  
            case Seq(_) => Future {
              System.err.println("*** [" + id + "] Closing session: " + ref)
              session.close()
              (toCall,i)
            }
          }
        } 
      }
     }}
    obs.onComplete({
      case Success((toCall,i)) => { 
        System.err.println("*** [" + id + "] Success: " + ref)
        try {
          toCall map runThread(i)
          promise.success(Unit)
        } catch {
          case (e:Exception) => promise.failure(e)
        }
      }
      case Failure(CASException) => System.err.println("*** [" + id + "] CAS Retry: " + ref + " - attempt: " + (attempt+1)); Thread.sleep(CAS_WAIT_MS); postResult(id,ref,res,attempt+1,promise)
      case Failure(e) => { 
        System.err.println("*** [" + id + "] Failed: " + ref + " - " + e.getLocalizedMessage)
        promise.failure(e)
      }
    })
  } catch {
    case (e:Exception) => { // this should never happen!
      System.err.println("*** [" + id + "] FATAL: " + ref)
      e.printStackTrace()
      publish(PiEventException(id,e))
      Future.failed(e)
    }
  }
  
  
  def postResult(i:PiInstance[ObjectId], ref:Int, res:PiObject, col:MongoCollection[PiInstance[ObjectId]], session:ClientSession):(Seq[(Int,PiFuture)],Observable[_]) = {
    System.err.println("*** [" + i.id + "] Handling result for thread " + ref + " : " + res)
    val unique = i.called
	  val ni = i.postResult(ref, res).reduce
	  if (ni.completed) ni.result match {
		  case None => {   
		    // Delete first, then announce the result, so that we don't do anything (like close the pool) early
		    System.err.println("*** [" + i.id + "] Completed with no result!")
		    (Seq(),(col.findOneAndDelete(session,and(equal("_id",i.id),equal("calls",unique))) andThen { case _ => publish(PiEventFailure(ni,ProcessExecutor.NoResultException(ni.id.toString())))}))
		  }
		  case Some(res) => {
		    System.err.println("*** [" + i.id + "] Completed with result!")
		    // Delete first, then announce the result, so that we don't do anything (like close the pool) early
		    (Seq(),(col.findOneAndDelete(session,and(equal("_id",i.id),equal("calls",unique))) andThen {case _ => publish(PiEventResult(i, res))}))
		  }
		} else {
		  System.err.println("*** [" + i.id + "] Handling threads after: " + ref)
		  val (toCall,resi) = ni.handleThreads(handleThread(ni)) // may throw exception!
		  val futureCalls = toCall flatMap (resi.piFutureOf)
		  System.err.println("*** [" + i.id + "] Updating state after: " + ref)
			(toCall zip futureCalls,col.findOneAndReplace(session,and(equal("_id",i.id),equal("calls",unique)), resi))
		}
  }
 
  
  def handleThread(i:PiInstance[ObjectId])(ref:Int,f:PiFuture):Boolean = {
     System.err.println("*** [" + i.id + "] Handling thread: " + ref + " (" + f.fun + ")")
    f match {
    case PiFuture(name, outChan, args) => i.getProc(name) match {
      case None => {
        System.err.println("*** [" + i.id + "] ERROR *** Unable to find process: " + name)
        throw ProcessExecutor.UnknownProcessException(name)
      }
      case Some(p:AtomicProcess) => true
      case Some(p:CompositeProcess) => { 
        System.err.println("*** [" + i.id + "] Executor encountered composite process thread: " + name)
        throw ProcessExecutor.AtomicProcessIsCompositeException(name) 
      }
    }
  } }
  
  
  def runThread(i:PiInstance[ObjectId])(t:(Int,PiFuture)):Unit = {
    System.err.println("*** [" + i.id + "] Handling thread: " + t._1 + " (" + t._2.fun + ")")
    t match {
    case (ref,PiFuture(name, outChan, args)) => i.getProc(name) match {
      case None => {
        // This should never happen! We already checked!
        System.err.println("*** [" + i.id + "] ERROR *** Unable to find process: " + name + " even though we checked already")
        publish(PiEventFailure(i,ProcessExecutor.UnknownProcessException(name)))
      }
      case Some(p:AtomicProcess) => {
        val objs = args map (_.obj)
        p.run(objs).onComplete{ 
          case Success(res) => {
            publish(PiEventReturn(i.id,ref,res))
            postResult(i.id,ref,res).recover({case t:Throwable => publish(PiEventFailure(i,t))})
          }
          case Failure (ex) => publish(PiEventProcessException(i.id,ref,ex))
        }
        System.err.println("*** [" + i.id + "] Called process: " + p.name + " ref:" + ref)
      }
      case Some(p:CompositeProcess) => {// This should never happen! We already checked! 
        System.err.println("*** [" + i.id + "] Executor encountered composite process thread: " + name)
        publish(PiEventFailure(i,ProcessExecutor.AtomicProcessIsCompositeException(name)))
      }
    }
  } }
  
  
  implicit class SafeObservable[T](obs: Observable[T])(implicit id:ObjectId) {
    def safeThen[U](f: Seq[T] => Future[U]): Future[U] = {
      val p = Promise[U]()
      obs.toFuture().onComplete({
        case Success(r:Seq[T]) => try {
          f(r).onComplete({
            case Success(s) => p.success(s)
            case Failure(e:Throwable) => p.failure(e)
          })
        } catch {
          case (exe:Exception) => p.failure(exe)
        }
        case Failure(ex:Throwable) => p.failure(ex)
      })
      p.future
    }
  }

  final case object CASException extends Exception("CAS")
  final case class CASFailureException(val id:String, val ref:Int, private val cause: Throwable = None.orNull)
                    extends Exception("Compare-and-swap failed after " + CAS_MAX_ATTEMPTS + " attempts for id: " + id + " - call id: " + ref, cause)  

  override def simulationReady:Boolean = false
  }



//class MongoFutureExecutor(client:MongoClient, db:String, collection:String, processes:PiProcessStore,timeout:Duration=10.seconds)(override implicit val context: ExecutionContext = ExecutionContext.global) 
//  extends MongoExecutor[PromiseHandler.ResultT](client, db, collection, new PromiseHandler[ObjectId], processes:PiProcessStore,timeout)(context) with FutureExecutor 
//    
//object MongoFutureExecutor {
//  def apply(client:MongoClient, db:String, collection:String, l:PiProcess*) = new MongoFutureExecutor(client,db,collection,SimpleProcessStore(l :_*))  
//}

    