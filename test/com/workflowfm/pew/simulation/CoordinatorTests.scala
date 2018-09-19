package com.workflowfm.pew.simulation

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestActors, TestKit }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent._
import scala.concurrent.Await
import scala.concurrent.duration._
import com.workflowfm.pew._
import com.workflowfm.pew.execution._


@RunWith(classOf[JUnitRunner])
class CoordinatorTests extends TestKit(ActorSystem("CoordinatorTests")) with WordSpecLike with Matchers with BeforeAndAfterAll {
  implicit val executionContext = ExecutionContext.global //system.dispatchers.lookup("akka.my-dispatcher")  
  implicit val timeout:FiniteDuration = 10.seconds

  override def afterAll:Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A Coordinator" must {
  
    "execute a simple task" in { 
  
    val resA = new TaskResource("A",1)
    val resB = new TaskResource("B",1)
      		
    val handler = MetricsOutputs(new MetricsPrinter())
    
    val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,handler,Seq(resA,resB)))
    
    val superevent = new TaskSimulation("SuperSim", coordinator, Seq("A","B"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
    val executor = new AkkaExecutor()
  
    coordinator ! Coordinator.AddSim(1,superevent,executor)
    coordinator ! Coordinator.Start
    expectNoMessage(200.millis)
    }
  }
}

