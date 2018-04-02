package com.workflowfm.pew.simulation

trait ValueGenerator[T] {
  def get :T
  def estimate :T
}

case class ConstantGenerator[T](value :T) extends ValueGenerator[T] {
  def get = value
  def estimate = value
}

case class UniformGenerator(min :Int, max:Int) extends ValueGenerator[Int] {
  def get = new util.Random().nextInt(max-min) + min
  def estimate = (max + min) / 2
}