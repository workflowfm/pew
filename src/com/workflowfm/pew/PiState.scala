package com.workflowfm.pew

import scala.annotation.tailrec

/**
 * This is the pi-calculus state of a running workflow. 
 * 
 * We separate inputs from outputs to make searching for matching channels more efficient.
 * We could have everything in one list like PiLib does.
 * 
 * @param inputs a map of Input type continuations based on their primary channels
 * @param outputs a map of Output type continuations based on their primary channels
 * @param calls a list of processes that have been called but the inputs haven't arrived yet
 * @param threads a map of processes that have been called and whose results we want. The
 *                calls to the actual processes should happen outside the PiState by a 
 *                process executor, so that things are kept clean and independent here. Each
 *                call has a unique reference number so that results don't clash.
 * @param threadCtr an integer to help create unique reference numbers for threads
 * @param freshCtr an integer to help create unique (fresh) channel names
 * @param processes a map of available processes, both atomic and composite, so that we know
 * 									how to handle calls when they occur
 * @param resources a ChanMap of unhandled resources. We try to keep this as clean as possible,
 * 									so that it only contains resources that need to be received by a pending call
 * 									(though this is not guaranteed, we might get some leftover garbage in here as well).
 */
case class PiState(inputs:Map[Chan,Input], outputs:Map[Chan,Output], calls:List[PiFuture], threads:Map[Int,PiFuture], threadCtr:Int, freshCtr:Int, processes:Map[String,PiProcess], resources:ChanMap) {
    
    /**
     * Introducing a term can have many different effects on the state. 
     * e.g. an Input term is simply added to the inputs, but a PiCut term adds new terms to both 
     * inputs and outputs and it affects freshCtr.
     * We also want to leave enough room for any future types of terms we might want to add.
     * We therefore defer the state effect to be determined by the Term itself.
     */
    def withTerm(t:Term):PiState = {
      //System.err.println("*** Handling term: " + t)
      t.addTo(this)
    }
    def withTerms(t:Seq[Term]):PiState = (this /: t)(_ withTerm _)
    
    def withSub(c:Chan,a:PiObject):PiState = copy(resources=resources + (c,a))
    def withSub(c:String,o:PiObject):PiState = withSub(Chan(c),o)
    def withSubs(m:Map[Chan,PiObject]) = copy(resources=resources ++ m)
    def withSubs(l:Seq[(Chan,PiObject)]) = copy(resources=resources ++ l)
    def withSubs(m:ChanMap) = copy(resources=resources ++ m)
    
    def withProc(p:PiProcess) = copy(processes = processes + (p.name->p))
    def withProcs(l:PiProcess*) = (this /: l)(_ withProc _)

    def withCalls(l:PiFuture*) = copy(calls = l.toList ++ calls)
    
    def withThread(ref:Int, name:String, chan:String, args:Seq[PiResource]) = withThreads((ref,PiFuture(name,Chan(chan),args)))
    def withThreads(t:(Int,PiFuture)*) = copy(threads = threads ++ (t map { x => x._1 -> x._2 }))
    def removeThread(ref:Int) = copy(threads=threads - ref)
    def removeThreads(refs:Iterable[Int]) = copy(threads=threads -- refs)
    
    def withTCtr(i:Int) = copy(threadCtr = i)
    def incTCtr() = copy(threadCtr = threadCtr + 1)    
    
    def withFCtr(i:Int) = copy(freshCtr = i)
    def incFCtr() = copy(freshCtr = freshCtr + 1)   
    
    private def removeIO(c:Chan):PiState = copy(inputs=inputs - c,outputs=outputs - c)
    
    /**
     * Performs either a single pi-calculus reduction or else handles the first available call with completed inputs.
     * The intersection of key sets detects common channels between inputs and outputs, which can then communicate.
     */
    def reduce():Option[PiState] = communicateFirst(inputs.keySet intersect outputs.keySet toList) match {
      case Some(s) => Some(s)
      case None => executeFirst()
    }
    
    /**
     * Perform all possible reductions to the state.
     */
    @tailrec
    final def fullReduce():PiState = reduce() match {
      case None => this
      case Some(s) => {
        System.err.println(s)
        s.fullReduce()
      }
    }
    
    /** 
     * Performs the first possible communication from a list of common channels.
     */
    @tailrec
    private def communicateFirst(l:List[Chan]):Option[PiState] = l match {
      case Nil => None
      case h :: t => communicate(h) match {
          case None => communicateFirst(t)
          case s => s
      }
    }
    
    /**
     * Attempts to perform a communication through channel k.
     * First uses the send method of the output to obtain the outputValue.
     * Then checks if the corresponding input can receive the outputValue.
     * It then uses the receive method of the input to pass the outputValue.
     * Finally it updates the state with continuations from both the input and output,
     * adds unhandled channel mappings to resources, and removes the input and output 
     * from their respective maps.
     */
    private def communicate(k:Chan) = {
       val (oterms,outputValue,newstate) = outputs.get(k).get.send(this)
       inputs.get(k) flatMap { i => 
         if (!i.admits(outputValue)) None
         else {
           System.err.println("*** Communicating [" + outputValue + "] through channel: " + k)
           val (iterms,newres) = inputs.get(k).get.receive(outputValue)
        	 Some(newstate withTerms oterms withTerms iterms withSubs newres removeIO k)
         }
       }
    }
    
    /**
     * Handles process calls.
     * For atomic process calls, we create an instantiated PiFuture and add it to the calls. 
     * We also generate and add the process input terms.
     * For composite process calls we instantiate the body and add it as a term.
     */
    def handleCall(c:PiCall):PiState = processes get c.name match {
      case None => {
        System.err.println("Failed to find process: " + c.name)
        this
      }
      case Some(p:AtomicProcess) => {
        System.err.println("*** Handling atomic call: " + c.name)
        val m = p.mapArgs(c.args:_*)
        copy(calls = p.getFuture(m) +: calls) withTerms p.getInputs(m)
      }
      case Some(p:CompositeProcess) => {
        //System.err.println("*** Handling composite call: " + c.name)
        val m = p.mapArgs(c.args:_*)
        this withTerm p.body.sub(m)
      }
    }
    
    /**
     * By 'execute' we mean converting a call to a thread.
     * This means checking a process in the calls list about whether its inputs have arrived.
     * This executes the first process (if any) in calls whose inputs have arrived.
     */
    def executeFirst():Option[PiState] = {
        def executeFirstRec(l:List[PiFuture]):Option[(PiState,List[PiFuture])] = l match {
          case Nil => None
          case h :: t => execute(h) match {
            case None => executeFirstRec(t) match {
              case None => None
              case Some((s,r)) => Some((s, h::r))
            }
            case Some(s) => Some((s,t))
          }
        }
        executeFirstRec(calls) match {
          case None => None
          case Some((s,l)) => Some(s.copy(calls = l))
        }
      }
    
    /**
     * This converts a PiFuture in the calls list to an instatiated thread to be executed externally. 
     * A unique thread id is generated using threadCtr. 
     * We also remove the mappings of all free channels in the process inputs from the resources list. 
     * These get used up in the process execution and are no longer needed. This helps keep the resources list clean.
     */
    def execute(p:PiFuture):Option[PiState] = {
      //val frees = (processes get p.fun toSeq) flatMap (_.inputFrees())
      val frees = p.args map (_.obj.frees)
      p.execute(resources) map { fut => copy(threads = threads + (threadCtr->fut),threadCtr = threadCtr + 1, resources = resources -- (frees.flatten :_*)) }
    }
    
    /**
     * This method should be used by the external executor to report the result of an executed thread and obtain the appropriate new state.
     * The PiFuture is removed from the threads list and its output Term is added, with the given object as the output resource.
     */
    def result(ref:Int,res:PiObject):Option[PiState] =
      threads get ref map { fut =>
        copy(threads = threads - ref) withTerm fut.toOutput(res)
      }
    
    /**
     * Executes a thread handling function over all threads.
     * Handler is expected to have side-effects (typically some Future trigger).
     * Handler should return true if a thread was handled successfully, false if it failed.
     * Returns the updated state containing only threads that were handled successfully.
     */
    def handleThreads(handler:((Int,PiFuture))=>Boolean) = copy(threads = threads filter handler)
}

object PiState {
  def apply(t:Term*):PiState = PiState(Map():Map[Chan,Input],Map():Map[Chan,Output],List(),Map():Map[Int,PiFuture],0,0,Map():Map[String,PiProcess],ChanMap()) withTerms t
  def apply(t:Seq[Term],l:Seq[(Chan,PiObject)]):PiState = PiState(t:_*) withSubs l
}

trait PiStateTester {
    
	def reduceOnce(t:ChannelTerm*) = PiState(t:_*).reduce()

	def reduce(t:ChannelTerm*):PiState = reduceState(PiState(t:_*))
	
	def reduceState(s:PiState):PiState = s.fullReduce()

	def reduceGet(r:String,t:ChannelTerm*):PiObject =
			reduceState(PiState(t:_*)).resources.obtain(Chan(r))
			
	def fState(l:(Chan,PiObject)*) = PiState(List(),l)
}