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
class CoordinatorTests extends TestKit(ActorSystem("CoordinatorTests")) with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {
  implicit val executionContext = ExecutionContext.global //system.dispatchers.lookup("akka.my-dispatcher")  
  implicit val timeout:FiniteDuration = 10.seconds

  override def afterAll:Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A Coordinator" must {
  
    val executor = new AkkaExecutor()  		
    //val handler = MetricsOutputs(new MetricsPrinter())
   
            //expectNoMessage(200.millis)
    "execute a simple task" in {   
      val resA = new TaskResource("A",1)
      val resB = new TaskResource("B",1)

      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq(resA,resB)))      
      val s = new TaskSimulation("S", coordinator, Seq("A","B"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      
      coordinator ! Coordinator.AddSim(0,s,executor)
      coordinator ! Coordinator.Start
      
      expectMsgType[Coordinator.Done](20.seconds).time should be (2)
    }
    
    "execute two independent tasks in parallel" in {   
      val resA = new TaskResource("A",1)
      val resB = new TaskResource("B",1)

      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq(resA,resB)))      
      val s1 = new TaskSimulation("S1", coordinator, Seq("A"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      val s2 = new TaskSimulation("S2", coordinator, Seq("B"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      
      coordinator ! Coordinator.AddSim(0,s1,executor)
      coordinator ! Coordinator.AddSim(0,s2,executor)
      coordinator ! Coordinator.Start
      
      expectMsgType[Coordinator.Done](20.seconds).time should be (2)
    }

    "queue two tasks with the same resource" in {   
      val resA = new TaskResource("A",1)

      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq(resA)))      
      val s1 = new TaskSimulation("S1", coordinator, Seq("A"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      val s2 = new TaskSimulation("S2", coordinator, Seq("A"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      
      coordinator ! Coordinator.AddSim(0,s1,executor)
      coordinator ! Coordinator.AddSim(0,s2,executor)
      coordinator ! Coordinator.Start
      
      expectMsgType[Coordinator.Done](20.seconds).time should be (4)
    }
  }
}

