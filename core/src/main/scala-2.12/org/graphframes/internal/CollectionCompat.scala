package org.graphframes.internal

import java.util.Collections
import scala.collection.JavaConverters.*
import scala.collection.mutable.ArrayBuffer

object CollectionCompat {
  @inline
  def sortBuffer[T](buffer: ArrayBuffer[T], ordering: Ordering[T]): Unit = {
    Collections.sort(buffer.asJava, ordering)
  }
}
