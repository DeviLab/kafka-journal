package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel, NonEmptySet => Nes}
import cats.effect.{Clock, Deferred, GenConcurrent, Resource}
import cats.effect.kernel.Resource.ExitCase
import cats.syntax.all._
import cats.effect.syntax.resource._
import cats.{Functor, ~>}
import com.evolutiongaming.catshelper.{BracketThrowable, Log, MeasureDuration}
import com.evolutiongaming.kafka.journal.conversions.ConsRecordToActionRecord
import com.evolutiongaming.kafka.journal.util.ResourcePool
import com.evolutiongaming.kafka.journal.util.StreamHelper._
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import com.evolutiongaming.sstream.Stream

import scala.concurrent.duration.FiniteDuration

trait ConsumeActionRecords[F[_]] {

  def apply(key: Key, partition: Partition, from: Offset): Stream[F, ActionRecord[Action]]
}

object ConsumeActionRecords {

  def apply[F[_] : BracketThrowable: Clock](
    consumerPool: ResourcePool[F, Journals.Consumer[F]],
    log: Log[F],
    poolMetrics: ConsumerPoolMetrics[F],
  )(implicit
    consRecordToActionRecord: ConsRecordToActionRecord[F],
    genConcurrent: GenConcurrent[F, Throwable]
  ): ConsumeActionRecords[F] = {
    (key: Key, partition: Partition, from: Offset) => {

      val topicPartition = TopicPartition(topic = key.topic, partition = partition)
      val topic = topicPartition.topic

      def seek(consumer: Journals.Consumer[F]) = {
        for {
          _ <- consumer.assign(Nes.of(topicPartition))
          _ <- consumer.seek(topicPartition, from)
          _ <- log.debug(s"$key consuming from $partition:$from")
        } yield {}
      }

      def filter(records: List[Nel[ConsRecord]]) = {
        for {
          records <- records
          record  <- records.toList if record.key.exists { _.value === key.id }
        } yield record
      }

      def poll(consumer: Journals.Consumer[F]) = {
        for {
          records0 <- consumer.poll
          records   = filter(records0.values.values.toList)
          actions  <- records.traverseFilter { a => consRecordToActionRecord(a).value }
        } yield actions
      }

      def consumerWithAcquisitionMetrics: Resource[F, Journals.Consumer[F]] =
        for {
          startedAcquiringAt <- Clock[F].monotonic.toResource
          deferred <- Deferred[F, FiniteDuration].toResource
          consumer <- consumerPool.borrow.onFinalize {
            for {
              start <- deferred.get
              end <- Clock[F].monotonic
              _ <- poolMetrics.useTime(topic, end - start)
            } yield ()
          }
          acquiredAt <- Clock[F].monotonic.toResource
          _ <- deferred.complete(acquiredAt).toResource
          _ <- poolMetrics.acquireTime(topic, acquiredAt - startedAcquiringAt).toResource
        } yield consumer

      for {
        consumer <- consumerWithAcquisitionMetrics.toStream
        _        <- seek(consumer).toStream
        records  <- Stream.repeat(poll(consumer))
        record   <- records.toStream1[F]
      } yield record
    }
  }

  implicit class ConsumeActionRecordsOps[F[_]](val self: ConsumeActionRecords[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): ConsumeActionRecords[G] = {
      (key: Key, partition: Partition, from1: Offset) => {
        self(key, partition, from1).mapK[G](fg, gf)
      }
    }
  }
}