package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.skafka.{Offset, Partition}

import scala.collection.immutable.Queue

object WithReadActionsOneByOne {

  def apply(actions: => Queue[ActionRecord[Action]]): WithReadActions[Async] = new WithReadActions[Async] {

    def apply[T](key: Key, partition: Partition, offset: Option[Offset])(f: ReadActions[Async] => Async[T]) = {

      val readActions = new ReadActions[Async] {

        var left = offset match {
          case None         => actions
          case Some(offset) => actions.dropWhile(_.offset <= offset)
        }

        def apply() = {
          left.dequeueOption.fold(Async.nil[ActionRecord[Action]]) { case (record, left) =>
            this.left = left
            List(record).async
          }
        }
      }

      f(readActions)
    }
  }
}