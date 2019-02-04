package com.workflowfm.pew.util

import scala.reflect.ClassTag

class ClassMap[CommonT]( elements: Seq[CommonT] ) {

  type ClassKey = Class[_ <: CommonT]

  protected val idMap: Map[String, Seq[ClassKey]]
    = elements
      .map( _.getClass )
      .groupBy[String]( _.getSimpleName )

  protected val classMap: Map[ClassKey, Seq[CommonT]]
    = elements
      .groupBy[ClassKey]( _.getClass )
      .withDefaultValue( Seq[CommonT]() )

  def apply[SubT <: CommonT]( implicit ct: ClassTag[SubT] ): Seq[SubT]
    = classMap( ct.runtimeClass.asInstanceOf[ClassKey] ).asInstanceOf[Seq[SubT]]

  def byName( name: String ): Seq[CommonT] = classMap( idMap( name ).head )

  def filter( fn: CommonT => Boolean ): ClassMap[CommonT]
    = new ClassMap[CommonT]( elements filter fn )

}

object ClassMap {

  def apply[T]( args: T* ): ClassMap[T] = new ClassMap( args.toSeq )

}
