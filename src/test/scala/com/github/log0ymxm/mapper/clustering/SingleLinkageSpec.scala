package com.github.log0ymxm.mapper.clustering

import breeze.linalg.DenseMatrix

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._

class ClusteringSpec extends FunSuite with SharedSparkContext {
  val distances: DenseMatrix[Double] = new DenseMatrix(8, 8, Array(
    0.00, 1.00, 0.26, 1.00, 0.38, 1.00, 0.58, 0.16,
    1.00, 0.00, 0.36, 0.29, 1.00, 0.32, 1.00, 0.19,
    0.26, 0.36, 0.00, 0.17, 1.00, 1.00, 0.40, 0.34,
    1.00, 0.29, 0.17, 0.00, 1.00, 1.00, 0.52, 1.00,
    0.38, 1.00, 1.00, 1.00, 0.00, 0.35, 1.00, 0.37,
    1.00, 0.32, 1.00, 1.00, 0.35, 0.00, 0.93, 0.28,
    0.58, 1.00, 0.40, 0.52, 1.00, 0.93, 0.00, 1.00,
    0.16, 0.19, 0.34, 1.00, 0.37, 0.28, 1.00, 0.00
  ))

  val expectedLinkage = Seq(
    (0, 7, 0.16, 2),
    (2, 3, 0.17, 2),
    (1, 7, 0.19, 3),
    (0, 2, 0.26, 5),
    (5, 7, 0.28, 6),
    (4, 5, 0.35, 7),
    (2, 6, 0.4, 8)
  )

  test("single linkage results") {
    val linkage = SingleLinkage(distances)
    assert(linkage(0) == (0, 7, 0.16, 2))
    assert(linkage(1) == (2, 3, 0.17, 2))
    assert(linkage(2) == (1, 7, 0.19, 3))
    assert(linkage(3) == (0, 2, 0.26, 5))
    assert(linkage(4) == (5, 7, 0.28, 6))
    assert(linkage(5) == (4, 5, 0.35, 7))
    assert(linkage(6) == (2, 6, 0.4, 8))
  }

  test("fcluster results") {
    assert(SingleLinkage.fcluster(expectedLinkage, 1) == Seq(0, 0, 0, 0, 0, 0, 0, 0))
    assert(SingleLinkage.fcluster(expectedLinkage, 2) == Seq(0, 0, 0, 0, 0, 0, 1, 0))
    assert(SingleLinkage.fcluster(expectedLinkage, 3) == Seq(0, 0, 0, 0, 1, 0, 2, 0))
    assert(SingleLinkage.fcluster(expectedLinkage, 4) == Seq(0, 0, 0, 0, 1, 2, 3, 0))
    assert(SingleLinkage.fcluster(expectedLinkage, 5) == Seq(0, 0, 1, 1, 2, 3, 4, 0))
    assert(SingleLinkage.fcluster(expectedLinkage, 6) == Seq(0, 1, 2, 2, 3, 4, 5, 0))
    assert(SingleLinkage.fcluster(expectedLinkage, 7) == Seq(0, 1, 2, 3, 4, 5, 6, 0))
    assert(SingleLinkage.fcluster(expectedLinkage, 8) == Seq(0, 1, 2, 3, 4, 5, 6, 7))
  }

}
