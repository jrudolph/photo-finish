package net.virtualvoid.fotofinish.util

import org.apache.pekko.stream.{ Attributes, Outlet, SourceShape }
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }

/**
 * A source that infinitely repeats the given source (by rematerializing it when the previous
 * instance was exhausted).
 *
 * Note that the elements can differ between rematerializations depending on the given source.
 */
class RepeatSource[T](source: Source[T, Any]) extends GraphStage[SourceShape[T]] {
  val out = Outlet[T]("RepeatSource.out")
  val shape: SourceShape[T] = SourceShape(out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    val waitingForInitialPull = new OutHandler {
      override def onPull(): Unit = {
        val in = new SubSinkInlet[T]("RepeatSource.in")
        val handler = supplying(in)
        in.setHandler(handler)
        setHandler(out, handler)
        in.pull()
        source.runWith(in.sink)(subFusingMaterializer)
      }
    }
    setHandler(out, waitingForInitialPull)

    def supplying(in: SubSinkInlet[T]): OutHandler with InHandler = new OutHandler with InHandler {
      override def onPull(): Unit = if (!in.hasBeenPulled) in.pull()
      override def onDownstreamFinish(cause: Throwable): Unit = {
        in.cancel(cause)
        super.onDownstreamFinish(cause)
      }

      override def onPush(): Unit = push(out, in.grab())
      override def onUpstreamFinish(): Unit = {
        setHandler(out, waitingForInitialPull)
        if (isAvailable(out)) waitingForInitialPull.onPull()
      }
      // just propagate failure
      // override def onUpstreamFailure(ex: Throwable): Unit = super.onUpstreamFailure(ex)
    }
  }
}