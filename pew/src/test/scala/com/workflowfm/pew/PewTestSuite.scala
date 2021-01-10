package com.workflowfm.pew

import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers, TryValues }

abstract class PewTestSuite extends FlatSpec with Matchers with TryValues with BeforeAndAfterAll
