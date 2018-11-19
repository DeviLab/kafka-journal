package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.util.concurrent.Executors

import com.evolutiongaming.cassandra
import com.evolutiongaming.cassandra.sync.AutoCreate
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.scassandra.Session

import scala.concurrent.ExecutionContext

trait CassandraSync[F[_]] {
  def apply[A](f: => F[A] /*TODO eager f*/): F[A]
}

object CassandraSync {

  def apply(schemaConfig: SchemaConfig)(implicit ec: ExecutionContext, session: Session): CassandraSync[Async] = {
    val keyspace = schemaConfig.keyspace
    val autoCreate = if (keyspace.autoCreate) AutoCreate.Table else AutoCreate.None
    apply(
      keyspace = keyspace.name,
      table = schemaConfig.locksTable,
      autoCreate = autoCreate)
  }

  def apply(
    keyspace: String,
    table: String,
    autoCreate: AutoCreate)(implicit ec: ExecutionContext, session: Session): CassandraSync[Async] = {

    new CassandraSync[Async] {

      def apply[A](f: => Async[A]) = {
        implicit val es = Executors.newScheduledThreadPool(2)
        val cassandraSync = cassandra.sync.CassandraSync(
          keyspace = keyspace,
          table = table,
          autoCreate = autoCreate)
        val future = cassandraSync("kafka-journal")(f.future)
        future.onComplete { _ => es.shutdown() }
        future.async
      }
    }
  }
}