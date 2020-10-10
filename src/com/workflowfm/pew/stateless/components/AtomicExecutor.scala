package com.workflowfm.pew.stateless.components

import com.workflowfm.pew._
import com.workflowfm.pew.stateless.StatelessMessages._
import org.bson.types.ObjectId

import scala.concurrent._

/** Runs AtomicProcesses and handles all necessary communication with the various stateless workflow actors.
  */
class AtomicExecutor(implicit exec: ExecutionContext)
    extends StatelessComponent[Assignment, Future[Seq[AnyMsg]]] {

  override def respond: Assignment => Future[Seq[AnyMsg]] = {
    case Assignment(pii, ref, name, args) =>
      def fail(piEx: PiException[ObjectId]): Future[SequenceFailure] =
        Future.successful(SequenceFailure(pii.id, ref, piEx))

      def failOther(t: Throwable): Future[SequenceFailure] = fail(
        RemoteProcessException[ObjectId](pii.id, ref.id, t)
      )

      try {
        val proc: MetadataAtomicProcess = pii.getAtomicProc(name)

        try {
          proc
            .runMeta(args.map(_.obj))
            .map({
              case (res, meta) =>
                Seq(
                  SequenceRequest(pii.id, (ref, res)),
                  PiiLog(PiEventReturn(pii.id, ref.id, res, meta))
                )
            })
            .recoverWith({ case t: Throwable => failOther(t).map(Seq(_)) })

        } catch {
          case t: Throwable =>
            println("AtomicExecutor: Pre-execution failed:")
            t.printStackTrace()

            // err(proc)(handling)(PreExecutionException(t)).map(Seq(_))
            failOther(PreExecutionException(t)).map(Seq(_))
        }

      } catch {
        case ex: UnknownProcessException[ObjectId] => fail(ex).map(Seq(_))
        case ex: AtomicProcessIsCompositeException[ObjectId] => fail(ex).map(Seq(_))
        case ex: Exception => failOther(ex).map(Seq(_))
      }
  }
}

case class PreExecutionException(t: Throwable) extends Throwable
