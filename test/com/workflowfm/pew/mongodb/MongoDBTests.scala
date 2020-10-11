package com.workflowfm.pew.mongodb

import com.workflowfm.pew._

import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.{ CodecProvider, CodecRegistry, CodecRegistries }
import org.bson.codecs.configuration.CodecRegistries.{ fromRegistries, fromProviders, fromCodecs }

import scala.concurrent.Await
import scala.concurrent.duration._

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala._
import com.workflowfm.pew.mongodb.bson.PiCodecProvider
import com.workflowfm.pew.mongodb.bson.PiBSONTestHelper
import org.mongodb.scala.model.Filters
import org.mongodb.scala.ClientSessionOptions
import com.mongodb.session.ClientSession

@RunWith(classOf[JUnitRunner])
class MongoDBTests
    extends FlatSpec
    with Matchers
    with PiBSONTestHelper
    with MongoDBTestHelper
    with PiStateTester {
  it should "connect and disconnect properly" in {
    val reg = fromRegistries(fromProviders(new PiCodecProvider), DEFAULT_CODEC_REGISTRY)

    val mongoClient: MongoClient = MongoClient()
    // Use a Connection String
    //val mongoClient: MongoClient = MongoClient("mongodb://localhost")

    val database: MongoDatabase = mongoClient.getDatabase("pew").withCodecRegistry(reg);
    val collection = database.getCollection("test_insts")

    val count = Await.result(collection.count().toFuture(), 1.minute)
    println("Count: " + count)

    mongoClient.close()
  }

//  it should "put new PiInstances as Documents" in {
//    val proc1 = DummyProcess("PROC", Seq("C","R"), "R", Seq((Chan("INPUT"),"C")))
//	  val procs = PiProcess.mapOf(proc1)
//
//    val reg = fromRegistries(fromProviders(new PiCodecProvider(procs)),DEFAULT_CODEC_REGISTRY)
//    val inst = PiInstance(new ObjectId,Seq(),proc1,PiState())
//
//    val mongoClient: MongoClient = MongoClient()
//    val database: MongoDatabase = mongoClient.getDatabase("pew").withCodecRegistry(reg);
//    val collection = database.getCollection("test_insts")
//    val doc = documentOf(reg.get(classOf[PiInstance[ObjectId]]), inst)
//
//    val obs = collection.insertOne(doc)
//
//    obs.subscribe(new Observer[Completed] {
//      override def onNext(result: Completed): Unit = println("Inserted")
//      override def onError(e: Throwable): Unit = println("Failed: " + e.getMessage)
//      override def onComplete(): Unit = { println("Completed"); mongoClient.close() }
//    })
//
//  }

  it should "put new PiInstances as Documents and read them back" in {
    val proc1 = DummyProcess("PROC", Seq("C", "R"), "R", Seq((Chan("INPUT"), "C")))
    val procs = SimpleProcessStore(proc1)

    val reg = fromRegistries(fromProviders(new PiCodecProvider(procs)), DEFAULT_CODEC_REGISTRY)
    val oid = new ObjectId
    val inst = PiInstance(oid, Seq(), proc1, PiState())

    val mongoClient: MongoClient = MongoClient()
    val database: MongoDatabase = mongoClient.getDatabase("pew").withCodecRegistry(reg);
    val collection = database.getCollection("test_insts")
    val doc = documentOf(reg.get(classOf[PiInstance[ObjectId]]), inst)

    val obs = collection.insertOne(doc)

    Await.result(obs.toFuture, 1.minute)

    val obs2 = collection.find(Filters.equal("_id", oid))

    val res = Await.result(obs2.toFuture, 1.minute)
    println("Result: " + res)
    mongoClient.close()

    res should not be empty
  }

  it should "put/get new PiInstances directly" in {
    val proc1 = DummyProcess("PROC", Seq("C", "R"), "R", Seq((Chan("INPUT"), "C")))
    val procs = SimpleProcessStore(proc1)

    val reg = fromRegistries(fromProviders(new PiCodecProvider(procs)), DEFAULT_CODEC_REGISTRY)
    val oid = new ObjectId
    val inst = PiInstance(oid, Seq(), proc1, PiState())

    val mongoClient: MongoClient = MongoClient()
    val database: MongoDatabase = mongoClient.getDatabase("pew").withCodecRegistry(reg);
    val collection: MongoCollection[PiInstance[ObjectId]] = database.getCollection("test_insts")

    val obs = collection.insertOne(inst)

    Await.result(obs.toFuture, 1.minute)

    val obs2 = collection.find(Filters.equal("_id", oid))

    val res = Await.result(obs2.toFuture, 1.minute)
    println("Result: " + res)
    mongoClient.close()

    res should not be empty
    res.head.id should be(oid)
    res.head.state should be(PiState())
  }

  it should "update PiInstances directly" in {
    val proc1 = DummyProcess("PROC", Seq("C", "R"), "R", Seq((Chan("INPUT"), "C")))
    val procs = SimpleProcessStore(proc1)

    val reg = fromRegistries(fromProviders(new PiCodecProvider(procs)), DEFAULT_CODEC_REGISTRY)
    val oid = new ObjectId
    val inst = PiInstance(oid, Seq(), proc1, PiState(Devour("C", "V"), Out("C", PiItem("OUTPUT"))))

    val mongoClient: MongoClient = MongoClient()
    val database: MongoDatabase = mongoClient.getDatabase("pew").withCodecRegistry(reg);
    val collection: MongoCollection[PiInstance[ObjectId]] = database.getCollection("test_insts")

    await(collection.insertOne(inst))

    val sessionQ =
      await(mongoClient.startSession(ClientSessionOptions.builder.causallyConsistent(true).build()))
    sessionQ should not be empty
    val session = sessionQ.head

    val i = await(collection.find(session, Filters.equal("_id", oid)))
    i should not be empty
    val done = await(collection.replaceOne(session, Filters.equal("_id", oid), i.head.reduce))
    done should not be empty
    session.close()

    val res = await(collection.find(Filters.equal("_id", oid)))
    res should not be empty
    println("Result: " + res)
    mongoClient.close()

    res should not be empty
    res.head.id should be(oid)
    res.head.state should be(fState((Chan("V"), PiItem("OUTPUT"))))
  }

  it should "update PiInstances with chained observables" in {
    val proc1 = DummyProcess("PROC", Seq("C", "R"), "R", Seq((Chan("INPUT"), "C")))
    val procs = SimpleProcessStore(proc1)

    val reg = fromRegistries(fromProviders(new PiCodecProvider(procs)), DEFAULT_CODEC_REGISTRY)
    val oid = new ObjectId
    val inst = PiInstance(oid, Seq(), proc1, PiState(Devour("C", "V"), Out("C", PiItem("OUTPUT"))))

    val mongoClient: MongoClient = MongoClient()
    val database: MongoDatabase = mongoClient.getDatabase("pew").withCodecRegistry(reg);
    val collection: MongoCollection[PiInstance[ObjectId]] = database.getCollection("test_insts")

    await(collection.insertOne(inst))

    val sessionQ =
      await(mongoClient.startSession(ClientSessionOptions.builder.causallyConsistent(true).build()))
    sessionQ should not be empty
    val session = sessionQ.head

    val obs = collection
      .find(session, Filters.equal("_id", oid))
      .flatMap({ i => collection.replaceOne(session, Filters.equal("_id", oid), i.reduce) })

    val done = await(obs)
    done should not be empty
    session.close()

    val res = await(collection.find(Filters.equal("_id", oid)))
    res should not be empty
    println("Result: " + res)
    mongoClient.close()

    res should not be empty
    res.head.id should be(oid)
    res.head.state should be(fState((Chan("V"), PiItem("OUTPUT"))))
  }
}

trait MongoDBTestHelper {
  def await[T](obs: Observable[T]) = Await.result(obs.toFuture, 1.minute)
}
