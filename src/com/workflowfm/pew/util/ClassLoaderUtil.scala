package com.workflowfm.pew.util

object ClassLoaderUtil {

  /** Execute a function on the current thread whilst using a specific ClassLoader.
    * Restores the previous ClassLoader before returning.
    *
    * @param tmpClassLoader ClassLoader to use during the function execution.
    * @param fnWrapped Function to execute with a different ClassLoader.
    * @tparam T The return type of `fnWrapped`
    * @return The returned value of `fnWrapped`
    */
  def withClassLoader[T](tmpClassLoader: ClassLoader)(fnWrapped: => T): T = {
    val pushedClassLoader = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(tmpClassLoader)
      fnWrapped
    } finally {
      Thread.currentThread().setContextClassLoader(pushedClassLoader)
    }
  }

}
