package com.shorrockin.cascal.utils

/**
 * utility class to make java.lang.ThreadLocal easier to use
 * courtesy to mehack (http://mehack.com/)
 */
class ThreadLocal[T](init: => T) extends java.lang.ThreadLocal[T] with Function0[T] {
  override def initialValue:T = init
  def apply = get
  def withValue[S](thunk:(T => S)):S = thunk(get)
}
