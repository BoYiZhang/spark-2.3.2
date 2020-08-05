package org.apache.sparktest


class SomethingNotSerializable {
  def someValue = 1
  def scope(name: String)(body: => Unit) = body
  def someMethod(): Unit = scope("one") {
    def x = someValue
    def y = 2
    scope("two") { println(y + 1) }
  }
}