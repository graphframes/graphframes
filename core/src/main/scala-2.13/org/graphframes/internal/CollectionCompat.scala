package org.graphframes.internal

import scala.collection.mutable.ArrayBuffer

object CollectionCompat {
  @inline
  def sortBuffer[T](buffer: ArrayBuffer[T], ordering: Ordering[T]): Unit = {
    buffer.sortInPlace()(ordering)
  }
}
