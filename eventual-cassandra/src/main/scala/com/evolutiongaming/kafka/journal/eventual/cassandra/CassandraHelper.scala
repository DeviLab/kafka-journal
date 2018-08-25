package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.nio.ByteBuffer
import java.time.Instant
import java.util.Date

import com.datastax.driver.core.{BoundStatement, ResultSet, Row}
import com.evolutiongaming.cassandra.CassandraHelper._
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.{Bytes, SeqNr}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

// TODO move to cassandra client
object CassandraHelper {

  implicit class BoundStatementOps(val self: BoundStatement) extends AnyVal {
    def encode[T](name: String, value: T)(implicit encode: Encode[T]): BoundStatement = {
      encode(self, name, value)
    }
  }


  implicit class RowOps(val self: Row) extends AnyVal {
    def decode[T](name: String)(implicit decode: Decode[T]): T = {
      decode(self, name)
    }
  }

  // TODO check performance of binding `by name`
  trait Encode[-T] {
    def apply(statement: BoundStatement, name: String, value: T): BoundStatement
  }

  // TODO check performance of binding `by name`
  trait Decode[T] extends {
    def apply(row: Row, name: String): T
  }


  trait Codec[T] extends Encode[T] with Decode[T]


  // TODO add codecs for all supported types

  implicit val StrCodec: Codec[String] = new Codec[String] {
    def apply(statement: BoundStatement, name: String, value: String) = statement.setString(name, value)
    def apply(row: Row, name: String) = row.getString(name)
  }

  implicit val IntCodec: Codec[Int] = new Codec[Int] {
    def apply(statement: BoundStatement, name: String, value: Int) = statement.setInt(name, value)
    def apply(row: Row, name: String) = row.getInt(name)
  }

  implicit val LongCodec: Codec[Long] = new Codec[Long] {
    def apply(statement: BoundStatement, name: String, value: Long) = statement.setLong(name, value)
    def apply(row: Row, name: String) = row.getLong(name)
  }

  implicit val InstantCodec: Codec[Instant] = new Codec[Instant] {
    def apply(statement: BoundStatement, name: String, value: Instant) = {
      val timestamp = Date.from(value)
      statement.setTimestamp(name, timestamp)
    }
    def apply(row: Row, name: String) = {
      val timestamp = row.getTimestamp(name)
      timestamp.toInstant
    }
  }

  implicit val BytesCodec: Codec[Bytes] = new Codec[Bytes] {
    def apply(statement: BoundStatement, name: String, value: Bytes) = {
      val bytes = ByteBuffer.wrap(value.value)
      statement.setBytes(name, bytes)
    }
    def apply(row: Row, name: String) = {
      val bytes = row.getBytes(name)
      Bytes(bytes.array())
    }
  }

  // TODO not bind to concrete type of element
  implicit val SetStrCodec: Codec[Set[String]] = new Codec[Set[String]] {

    def apply(statement: BoundStatement, name: String, value: Set[String]) = {
      val set = value.asJava
      statement.setSet(name, set, classOf[String])
    }

    def apply(row: Row, name: String) = {
      val set = row.getSet(name, classOf[String])
      set.asScala.toSet
    }
  }

  // TODO move out
  implicit val SeqNrCodec: Codec[SeqNr] = new Codec[SeqNr] {
    def apply(statement: BoundStatement, name: String, value: SeqNr) = LongCodec(statement, name, value.value)
    def apply(row: Row, name: String) = SeqNr(LongCodec(row, name))
  }

  implicit class ResultSetOps(val self: ResultSet) extends AnyVal {

    def foldWhile[S](fetchThreshold: Int, s: S)(f: Fold[S, Row])(implicit ec: ExecutionContext /*TODO remove*/): Async[Switch[S]] = {

      @tailrec def foldWhile(s: S, available: Int): Switch[S] = {
        if (available == 0) s.continue
        else {
          if (available == fetchThreshold) self.fetchMoreResults()
          val row = self.one()
          val switch = f(s, row)
          if (switch.stop) switch
          else foldWhile(switch.s, available - 1)
        }
      }

      val fetch = (s: Switch[S]) => {
        val fetched = self.isFullyFetched
        val available = self.getAvailableWithoutFetching
        val ss = foldWhile(s.s, available)
        if (ss.stop || fetched) Switch.stop(ss).async
        else for {
          _ <- self.fetchMoreResults().asScala().async
        } yield Switch.continue(ss)
      }

      fetch.foldWhile(s.continue)
    }
  }
}
