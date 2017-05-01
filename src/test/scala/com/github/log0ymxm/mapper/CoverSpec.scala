package com.github.log0ymxm.mapper

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }

import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext

class CoverSpec extends FunSuite with SharedSparkContext {
  test("cover") {
    val rdd = sc.parallelize((0 to 10).toSeq)
    val filtration = new IndexedRowMatrix(
      rdd.map({ x =>
        new IndexedRow(x, new DenseVector(Array(x * 2, scala.math.sin(x))))
      })
    )

    val cover = new Cover(filtration, 4, 0.5)

    assert(cover.numCoverSegments == 16)
    assert(cover.filterRanges(0) == NumericBoundary(0.0, 20.0))
    assert(cover.filterRanges(1).lower >= -1.0)
    assert(cover.filterRanges(1).upper <= 1.0)

    assert(cover.coverAssignment(new DenseVector(Array(8.33, 0.5))) == List(CoverSegmentKey(6), CoverSegmentKey(7)))

  }
}
