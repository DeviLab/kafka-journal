package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.{Monad, Order}
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._

trait CreateTables[F[_]] {
  import CreateTables.{Fresh, Table}

  def apply(keyspace: String, tables: Nel[Table]): F[Fresh]
}


object CreateTables { self =>

  type Fresh = Boolean


  def apply[F[_]](implicit F: CreateTables[F]): CreateTables[F] = F


  def apply[F[_] : Monad : CassandraCluster : CassandraSession : CassandraSync](
    log: Log[F]
  ): CreateTables[F] = new CreateTables[F] {

    def apply(keyspace: String, tables: Nel[Table]) = {

      def missing(keyspace: KeyspaceMetadata[F]) = {
        for {
          tables <- tables.foldLeftM(List.empty[Table]) { (create, table) =>
            for {
              metadata <- keyspace.table(table.name)
            } yield {
              metadata.fold(table :: create) { _ => create }
            }
          }
        } yield {
          tables.reverse
        }
      }

      def create(tables1: Nel[Table]) = {
        val fresh = tables1.length === tables.length
        for {
          _ <- log.info(s"tables: ${tables1.map(_.name).mkString_(",")}, fresh: $fresh")
          _ <- CassandraSync[F].apply { tables1.foldMapM { _.queries.foldMapM { _.execute.first.void } } }
        } yield fresh
      }

      for {
        tables   <- tables.distinct.pure[F]
        metadata <- CassandraCluster[F].metadata
        keyspace <- metadata.keyspace(keyspace)
        tables1  <- keyspace.fold(tables.toList.pure[F])(missing)
        fresh    <- tables1.toNel.fold(false.pure[F])(create)
      } yield fresh
    }
  }


  def of[F[_] : Monad : CassandraCluster : CassandraSession : CassandraSync : LogOf]: F[CreateTables[F]] = {
    for {
      log <- LogOf[F].apply(self.getClass)
    } yield {
      apply[F](log)
    }
  }


  def const[F[_]](fresh: F[Fresh]): CreateTables[F] = (_: String, _: Nel[Table]) => fresh


  final case class Table(name: String, queries: Nel[String])

  object Table {

    implicit val orderTable: Order[Table] = Order.by { a: Table => a.name }

    def apply(name: String, query: String): Table = Table(name, Nel.of(query))
  }
}