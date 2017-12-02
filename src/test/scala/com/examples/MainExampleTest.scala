package com.examples

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class MainExampleTest extends FunSuite with SharedSparkContext {

  test("testCreateRDDOfNRecords") {
    var N = 10

    var rdd = MainExample.createRDDOfNRecords(sc, N)
    assert(rdd.count() == N)

    N = 0
    rdd = MainExample.createRDDOfNRecords(sc, N)
    assert(rdd.count() == N)

    N = -1
    rdd = MainExample.createRDDOfNRecords(sc, N)
    assert(rdd.count() != N)

  }


}
