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
import com.workflowfm.pew.metrics.MetricsPrinter
import com.workflowfm.pew.metrics.MetricsOutputs
import com.workflowfm.pew.metrics.MetricsActor


@RunWith(classOf[JUnitRunner])
class CoordinatorTests extends TestKit(ActorSystem("CoordinatorTests")) with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {
  implicit val executionContext = ExecutionContext.global //system.dispatchers.lookup("akka.my-dispatcher")  
  implicit val timeout:FiniteDuration = 10.seconds

  override def afterAll:Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The Coordinator" must {
  
    val executor = new AkkaExecutor()  		
    val handler = MetricsOutputs(new MetricsPrinter())
   
            //expectNoMessage(200.millis)
    "execute a simple task" in {   
      val resA = new TaskResource("A",1)
      val resB = new TaskResource("B",1)

      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq(resA,resB)))      
      val s = new TaskSimulation("S", coordinator, Seq("A","B"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      
      coordinator ! Coordinator.AddSim(1,s,executor)
      coordinator ! Coordinator.Start
      
      val done = expectMsgType[Coordinator.Done](20.seconds)
      done.time should be (3)
    }
    
    "execute two independent tasks in parallel" in {   
      val resA = new TaskResource("A",1)
      val resB = new TaskResource("B",1)

      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq(resA,resB)))      
      val s1 = new TaskSimulation("S1", coordinator, Seq("A"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      val s2 = new TaskSimulation("S2", coordinator, Seq("B"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      
      coordinator ! Coordinator.AddSim(1,s1,executor)
      coordinator ! Coordinator.AddSim(1,s2,executor)
      coordinator ! Coordinator.Start
      
      val done = expectMsgType[Coordinator.Done](20.seconds)
      done.time should be (3)
    }

    "queue two tasks with the same resource" in {   
      val resA = new TaskResource("A",1)

      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq(resA)))      
      val s1 = new TaskSimulation("S1", coordinator, Seq("A"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      val s2 = new TaskSimulation("S2", coordinator, Seq("A"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      
      coordinator ! Coordinator.AddSim(1,s1,executor)
      coordinator ! Coordinator.AddSim(1,s2,executor)
      coordinator ! Coordinator.Start
      
      val done = expectMsgType[Coordinator.Done](20.seconds)
      done.time should be (5)
    }
    
    "measure delays and idling appropriately" in {   
      val resA = new TaskResource("A",1)
      val resB = new TaskResource("B",1)

      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq(resA,resB)))      
      val s1 = new TaskSimulation("S1", coordinator, Seq("A"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      val s2 = new TaskSimulation("S2", coordinator, Seq("A","B"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Medium)
      
      coordinator ! Coordinator.AddSim(1,s1,executor)
      coordinator ! Coordinator.AddSim(1,s2,executor)
      coordinator ! Coordinator.Start
      
      val done = expectMsgType[Coordinator.Done](20.seconds)
      done.time should be (5)
      val metricB = done.metrics.resourceMetrics.find { x => x._1.equals("B") } 
      metricB should not be empty 
      metricB map { x => x._2.idle should be (2) }
      val metricS2T = done.metrics.taskMetrics.find { x => x._1.equals("S2Task(S2)") } 
      metricS2T should not be empty
      metricS2T map { x => x._2.delay should be (2) }
    }
    
    "measure intermediate delays and idling appropriately" in {   
      val resA = new TaskResource("A",1)
      val resB = new TaskResource("B",1)

      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq(resA,resB)))      
      val s1 = new TaskSimulation("S1", coordinator, Seq("A"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      val s2 = new TaskSimulation("S2", coordinator, Seq("B"), new ConstantGenerator(3), new ConstantGenerator(2), -1, Task.Highest)
      val s3 = new TaskSimulation("S3", coordinator, Seq("A","B"), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Medium)
      
      coordinator ! Coordinator.AddSim(1,s1,executor)
      coordinator ! Coordinator.AddSim(1,s2,executor)
      coordinator ! Coordinator.AddSim(2,s3,executor)
      coordinator ! Coordinator.Start
      
      val done = expectMsgType[Coordinator.Done](20.seconds)
      
      done.time should be (6)
      val metricA = done.metrics.resourceMetrics.find { x => x._1.equals("A") } 
      metricA should not be empty 
      metricA map { x => x._2.idle should be (1) }
      val metricS3T = done.metrics.taskMetrics.find { x => x._1.equals("S3Task(S3)") } 
      metricS3T should not be empty
      metricS3T map { x => x._2.delay should be (2) }
    }
    
    "run a task with no resources" in {
      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq()))      
      val s1 = new TaskSimulation("S1", coordinator, Seq(), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      
      coordinator ! Coordinator.AddSim(1,s1,executor)
      coordinator ! Coordinator.Start
      
      val done = expectMsgType[Coordinator.Done](20.seconds)
      handler(done.time,done.metrics)
      
      done.time should be (3)
      done.metrics.resourceMetrics.isEmpty should be (true)
      done.metrics.simulationMetrics.size should be (1)
      done.metrics.taskMetrics.size should be (1)
    }
    
    "run multiple tasks with no resources" in {
      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq()))      
      val s1 = new TaskSimulation("S1", coordinator, Seq(), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      val s2 = new TaskSimulation("S2", coordinator, Seq(), new ConstantGenerator(2), new ConstantGenerator(2), -1, Task.Highest)
      
      coordinator ! Coordinator.AddSim(1,s1,executor)
      coordinator ! Coordinator.AddSim(1,s2,executor)
      coordinator ! Coordinator.Start
      
      val done = expectMsgType[Coordinator.Done](20.seconds)
      handler(done.time,done.metrics)
      
      done.time should be (3)
      done.metrics.resourceMetrics.isEmpty should be (true)
      done.metrics.simulationMetrics.size should be (2)
      done.metrics.taskMetrics.size should be (2)
    }
  }
  
  "The MetricsActor" must {
  
    val executor = new AkkaExecutor()  		
    val handler = MetricsOutputs(new MetricsPrinter())
   
            //expectNoMessage(200.millis)
    "work properly" in {   
      println ("*** MetricsActor results should appear here:")
      
      val resA = new TaskResource("A",1)

      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq(resA)))      
      val s = new TaskSimulation("S", coordinator, Seq("A"), new ConstantGenerator(1), new ConstantGenerator(2), -1, Task.Highest)
      
      coordinator ! Coordinator.AddSim(1,s,executor)
      
      val metricsActor = system.actorOf(MetricsActor.props(handler))
      metricsActor ! MetricsActor.Start(coordinator)
      
      expectNoMessage(200.millis)
    }
    
    "start multiple simulations with one message" in {   
      println ("*** MetricsActor multiple results should appear here:")
      
      val resA = new TaskResource("A",1)

      val coordinator = system.actorOf(Coordinator.props(DefaultScheduler,Seq(resA)))      
      val s1 = new TaskSimulation("S1", coordinator, Seq("A"), new ConstantGenerator(1), new ConstantGenerator(2), -1, Task.Highest)
      val s2 = new TaskSimulation("S2", coordinator, Seq("A"), new ConstantGenerator(1), new ConstantGenerator(2), -1, Task.Highest)
      
      val metricsActor = system.actorOf(MetricsActor.props(handler))
      metricsActor ! MetricsActor.StartSims(coordinator,Seq((1,s1),(1,s2)),executor)
      
      expectNoMessage(200.millis)
    }
  }
}

