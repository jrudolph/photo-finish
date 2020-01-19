package net.virtualvoid.fotofinish.util

import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }

class StatefulDetachedFlow[T, U, S](initialState: () => S, handle: (S, T) => S, emitF: S => (S, Vector[U]), isFinished: S => Boolean) extends GraphStage[FlowShape[T, U]] {
  val in = Inlet[T]("StateFullDetachedFlow.in")
  val out = Outlet[U]("StateFullDetachedFlow.out")
  val shape: FlowShape[T, U] = FlowShape(in, out)
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    setHandlers(in, out, this)

    override def preStart(): Unit = pull(in)

    private[this] var state = initialState()

    override def onPush(): Unit = {
      state = handle(state, grab(in))
      if (isFinished(state)) completeStage()
      else {
        pull(in)
        if (isAvailable(out)) onPull()
      }
    }
    override def onPull(): Unit = {
      val (newState, toEmit) = emitF(state)
      state = newState
      if (toEmit.nonEmpty) emitMultiple(out, toEmit)
      if (isFinished(state)) completeStage()
    }
  }
}