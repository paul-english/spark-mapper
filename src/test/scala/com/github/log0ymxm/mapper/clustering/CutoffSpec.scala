package com.github.log0ymxm.mapper.clustering

import org.scalatest._

class CutoffSpec extends FunSuite {
  val diameter = 1.00
  val linkage = Seq(
    (0, 7, 0.16, 2),
    (2, 3, 0.17, 2),
    (1, 7, 0.19, 3),
    (0, 2, 0.26, 5),
    (5, 7, 0.28, 6),
    (4, 5, 0.35, 7),
    (2, 6, 0.4, 8)
  )

  test("histogram cutoff") {
    assert(1 == Cutoff.histogramGap(linkage, diameter, 3))
    assert(2 == Cutoff.histogramGap(linkage, diameter, 5))
    assert(2 == Cutoff.histogramGap(linkage, diameter, 10))
  }

  test("first gap cutoff") {
    assert(6 == Cutoff.firstGap(linkage, diameter, 0.05))
    assert(2 == Cutoff.firstGap(linkage, diameter, 0.1))
    assert(1 == Cutoff.firstGap(linkage, diameter, 0.6))
  }

  test("biggest gap cutoff") {
    assert(1 == Cutoff.biggestGap(linkage, diameter))
  }

}
