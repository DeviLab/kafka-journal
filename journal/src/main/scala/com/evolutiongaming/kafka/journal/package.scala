package com.evolutiongaming.kafka

package object journal {

  // TODO rename to not conflict with cats
  type Id = String

  // TODO remove
  type Bytes = Array[Byte]

  type Tag = String

  type Tags = Set[Tag]

  type Headers = Map[String, String]
}
