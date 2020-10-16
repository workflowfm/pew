package com.workflowfm.pew.util

import scala.reflect.ClassTag

class ClassMap[CommonT](elements: Seq[CommonT]) {

  type ClassKey = Class[_ <: CommonT]

  protected val idMap: Map[String, Seq[ClassKey]] = elements
    .map(_.getClass)
    .groupBy[String](_.getSimpleName)

  protected val classMap: Map[ClassKey, Seq[CommonT]] = elements
    .groupBy[ClassKey](_.getClass)
    .withDefaultValue(Seq[CommonT]())

  /** Retrieve all elements in the ClassMap which are assignable from `SubT`.
    *
    * @return A sequence of elements of type `SubT`.
    */
  def apply[SubT <: CommonT](implicit ct: ClassTag[SubT]): Seq[SubT] = {
    val rtClass: ClassKey = ct.runtimeClass.asInstanceOf[ClassKey]

    def getMembers(key: ClassKey, instances: Seq[CommonT]): Seq[SubT] = {
      if (rtClass isAssignableFrom key)
        instances map (_.asInstanceOf[SubT])
      else
        Seq()
    }

    classMap.flatMap((getMembers _).tupled).toSeq
  }

  def byName(name: String): Seq[CommonT] = classMap(idMap(name).head)

  def filter(fn: CommonT => Boolean): ClassMap[CommonT] = new ClassMap[CommonT](elements filter fn)

}

object ClassMap {

  def apply[T](args: T*): ClassMap[T] = new ClassMap(args.toSeq)

}
