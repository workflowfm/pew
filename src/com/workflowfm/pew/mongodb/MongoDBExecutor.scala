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

class MongoDBExecutor(client:MongoClient, db:String, collection:String, processes:Map[String,PiProcess],timeout:Duration=10.seconds)(override implicit val context: ExecutionContext = ExecutionContext.global) extends FutureExecutor {
  def this(client:MongoClient, db:String, collection:String, l:PiProcess*) = this(client,db,collection,PiProcess.mapOf(l :_*))  
   
  val codecRegistry = fromRegistries(fromProviders(new PiCodecProvider(processes)),DEFAULT_CODEC_REGISTRY)
  val database: MongoDatabase = client.getDatabase(db).
    withCodecRegistry(codecRegistry).
    //withReadConcern(ReadConcern.LINEARIZABLE).
    withWriteConcern(WriteConcern.MAJORITY);
  val col:MongoCollection[PiInstance[ObjectId]] = database.getCollection(collection)
  
  val handler = new PromiseHandler[ObjectId]  
    
  def await[T](obs:Observable[T]) = Await.result(obs.toFuture,timeout)
  def await[T](f:Future[T]) = Await.result(f,timeout)
  
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
		  val res = ni.handleThreads(handleThread(ni.id))
      await(col.insertOne(res))
	  }
	  ret
  }
  
  
  //@tailrec
  final def postResult(id:ObjectId, ref:Int, res:PiObject):Unit = {
		System.err.println("*** [" + id + "] Got result for thread " + ref + " : " + res)
    implicit val iid:ObjectId = id   
    
    val obs = client.startSession(ClientSessionOptions.builder.causallyConsistent(true).build()) safeThen { 
      case Seq(session:ClientSession) => { 
      System.err.println("*** [" + id + "] Session started: " + ref)
      col.find(session,equal("_id",id)) safeThen {
        case Seq() => {
          System.err.println("*** [" + id + "] CAS: " + ref)
          session.close()
          Future.failed(CASException)
        }
        case Seq(i:PiInstance[ObjectId]) => {
        System.err.println("*** [" + id + "] Got PiInstance: " + i)
        postResult(i,ref,res,col,session).recover({ case ex => handler.failure(i,ex)}) safeThen {
          case Seq() => {
            System.err.println("*** [" + id + "] CAS: " + ref + " - Closing session")
            session.close()
            Future.failed(CASException)
          }  
          case Seq(_) => {
            System.err.println("*** [" + id + "] Closing session: " + ref + " - Closing session")
            session.close()
            Future.successful(Unit)
          }
        }
      }}
    }}
    obs.onComplete({
      case Success(_) => System.err.println("*** [" + id + "] Success: " + ref); Unit
      case Failure(CASException) => System.err.println("*** [" + id + "] CAS Retry: " + ref); Thread.sleep(1); postResult(id,ref,res)
      case Failure(e) => System.err.println("*** [" + id + "] Failed: " + ref); handler.failure(id,e)
    })
  }
  
  def postResult(i:PiInstance[ObjectId], ref:Int, res:PiObject, col:MongoCollection[PiInstance[ObjectId]], session:ClientSession):Observable[_] = {
    System.err.println("*** [" + i.id + "] Handling result for thread " + ref + " : " + res)
    val unique = i.called
	  val ni = i.postResult(ref, res).reduce
	  if (ni.completed) ni.result match {
		  case None => {   
		    // Delete first, then announce the result, so that we don't do anything (like close the pool) early
		    col.deleteOne(session,and(equal("_id",i.id),equal("calls",unique))) andThen { case _ => handler.failure(ni,ProcessExecutor.NoResultException(ni.id.toString()))}
		  }
		  case Some(res) => {
		    // Delete first, then announce the result, so that we don't do anything (like close the pool) early
		    col.deleteOne(session,and(equal("_id",i.id),equal("calls",unique))) andThen {case _ => handler.success(i,res)}
		  }
		} else {
			col.replaceOne(session,and(equal("_id",i.id),equal("calls",unique)), ni.handleThreads(handleThread(ni.id)))
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
  
  override def execute(process:PiProcess,args:Seq[Any]):Future[Option[Any]] =
    call(process,args map PiObject.apply :_*)
   
}


    