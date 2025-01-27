package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.syntax.all._
import cats.Applicative
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.conversions.{ConsRecordToActionRecord, KafkaRead}
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.util.TemporalHelper._

import java.time.Instant
import scala.concurrent.duration.FiniteDuration


trait ReplicateRecords[F[_]] {

  def apply(records: Nel[ConsRecord], timestamp: Instant): F[Unit]
}

object ReplicateRecords {

  def apply[F[_]: BracketThrowable: Clock, A](
    consRecordToActionRecord: ConsRecordToActionRecord[F],
    journal: ReplicatedKeyJournal[F],
    metrics: TopicReplicatorMetrics[F],
    kafkaRead: KafkaRead[F, A],
    eventualWrite: EventualWrite[F, A],
    log: Log[F]
  ): ReplicateRecords[F] = {

    (records: Nel[ConsRecord], timestamp: Instant) => {

      def apply(records: Nel[ActionRecord[Action]]) = {
        val record = records.last
        val key = record.action.key
        val id = key.id

        def measurements(records: Int) = {
          for {
            now <- Clock[F].instant
          } yield {
            val timestamp1 = record.action.timestamp
            TopicReplicatorMetrics.Measurements(
              replicationLatency = now diff timestamp1,
              deliveryLatency = timestamp diff timestamp1,
              records = records)
          }
        }

        def append(partitionOffset: PartitionOffset, records: Nel[ActionRecord[Action.Append]]) = {

          val bytes = records.foldLeft(0L) { case (bytes, record) => bytes + record.action.payload.size }

          val events = records.flatTraverse { record =>
            val action = record.action
            val payloadAndType = action.toPayloadAndType
            val events = kafkaRead(payloadAndType).adaptError { case e =>
              JournalError(s"ReplicateRecords failed for id: $id, offset: $partitionOffset: $e", e)
            }
            for {
              events         <- events
              eventualEvents <- events.events.traverse { _.traverse { a => eventualWrite(a) } }
            } yield for {
              event <- eventualEvents
            } yield {
              EventRecord(record, event, events.metadata)
            }
          }

          def msg(
            events: Nel[EventRecord[EventualPayloadAndType]],
            latency: FiniteDuration,
            expireAfter: Option[ExpireAfter]
          ) = {
            val seqNrs =
              if (events.tail.isEmpty) s"seqNr: ${ events.head.seqNr }"
              else s"seqNrs: ${ events.head.seqNr }..${ events.last.seqNr }"
            val origin = records.head.action.origin
            val originStr = origin.foldMap { origin => s", origin: $origin" }
            val version = records.last.action.version
            val versionStr = version.fold("none") { _.toString }
            val expireAfterStr = expireAfter.foldMap { expireAfter => s", expireAfter: $expireAfter" }
            s"append in ${ latency.toMillis }ms, id: $id, offset: $partitionOffset, $seqNrs$originStr, version: $versionStr$expireAfterStr"
          }

          def measure(events: Nel[EventRecord[EventualPayloadAndType]], expireAfter: Option[ExpireAfter]) = {
            for {
              measurements <- measurements(records.size)
              result       <- metrics.append(events = events.length, bytes = bytes, measurements = measurements)
              _            <- log.info(msg(events, measurements.replicationLatency, expireAfter))
            } yield result
          }

          for {
            events       <- events
            expireAfter   = events.last.metadata.payload.expireAfter
            appended     <- journal.append(partitionOffset, timestamp, expireAfter, events)
            result       <- if (appended) measure(events, expireAfter) else Applicative[F].unit
          } yield result
        }

        def delete(partitionOffset: PartitionOffset, deleteTo: DeleteTo, origin: Option[Origin], version: Option[Version]) = {

          def msg(latency: FiniteDuration) = {
            val originStr = origin.foldMap { origin => s", origin: $origin" }
            val versionStr = version.fold("none") { _.toString }
            s"delete in ${ latency.toMillis }ms, id: $id, offset: $partitionOffset, deleteTo: $deleteTo$originStr, version: $versionStr"
          }

          def measure() = {
            for {
              measurements <- measurements(1)
              latency       = measurements.replicationLatency
              _            <- metrics.delete(measurements)
              result       <- log.info(msg(latency))
            } yield result
          }

          for {
            deleted <- journal.delete(partitionOffset, timestamp, deleteTo, origin)
            result  <- if (deleted) measure() else Applicative[F].unit
          } yield result
        }

        def purge(partitionOffset: PartitionOffset, origin: Option[Origin], version: Option[Version]) = {

          def msg(latency: FiniteDuration) = {
            val originStr = origin.foldMap { origin => s", origin: $origin" }
            val versionStr = version.fold("none") { _.toString }
            s"purge in ${ latency.toMillis }ms, id: $id, offset: $partitionOffset$originStr, version: $versionStr"
          }

          def measure() = {
            for {
              measurements <- measurements(1)
              latency       = measurements.replicationLatency
              _            <- metrics.purge(measurements)
              result       <- log.info(msg(latency))
            } yield result
          }

          for {
            purged <- journal.purge(partitionOffset.offset, timestamp)
            result <- if (purged) measure() else Applicative[F].unit
          } yield result
        }

        Batch
          .of(records)
          .foldMapM {
            case batch: Batch.Appends => append(batch.partitionOffset, batch.records)
            case batch: Batch.Delete  => delete(batch.partitionOffset, batch.to, batch.origin, batch.version)
            case batch: Batch.Purge   => purge(batch.partitionOffset, batch.origin, batch.version)
          }
      }

      for {
        records <- records.toList.traverseFilter { a => consRecordToActionRecord(a).value }
        result  <- records.toNel.foldMapM { records => apply(records) }
      } yield result
    }
  }
}
