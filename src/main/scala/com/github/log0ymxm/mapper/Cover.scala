package com.github.log0ymxm.mapper

import breeze.linalg.linspace
import mapper.utils.Utils
import org.apache.spark.Partitioner
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix

import scala.math.{ max, min }

case class NumericBoundary(val lower: Double, val upper: Double)
case class CoverSegmentKey(val id: Int) extends AnyVal

class CoverAssignmentPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[CoverSegmentKey]
    k.id
  }

  override def equals(other: Any): Boolean = other match {
    case dnp: CoverAssignmentPartitioner => dnp.numPartitions == numPartitions
    case _ => false
  }

}

class Cover(val filterValues: IndexedRowMatrix, val coverIntervals: Int, val coverOverlapRatio: Double) extends Serializable {
  assert(0 <= coverOverlapRatio && coverOverlapRatio <= 1)

  val k = filterValues.numCols();
  val numCoverSegments: Int = Math.pow(coverIntervals, k).toInt;

  // Compute the min and max of each filter applied to our dataset
  // allowing us to pick the bounding box of the cover.
  val filterStats = filterValues.toRowMatrix().computeColumnSummaryStatistics()
  val filterRanges: Array[NumericBoundary] = (filterStats.min.toArray, filterStats.max.toArray)
    .zipped
    .map({ case (min, max) => NumericBoundary(min, max) })

  // For each filter dimension k, compute a linear space that will
  // be used to build an overlapping square cover
  val filterLinspaces: Seq[Array[Double]] = filterRanges.map({
    (bound) => linspace(bound.lower, bound.upper, coverIntervals + 1).toArray
  })

  // Compute overlapping sliding windows over each filter dimension
  val filterOverlaps: Traversable[Seq[NumericBoundary]] = (filterRanges, filterLinspaces)
    .zipped
    .map({
      case (bound, linValues) =>
        val overlapSize = ((bound.upper - bound.lower) / (coverIntervals)) * coverOverlapRatio
        linValues.sliding(2, 1).map({
          case (window) =>
            NumericBoundary(
              max(bound.lower, window(0) - (overlapSize / 2)),
              min(bound.upper, window(1) + (overlapSize / 2))
            )
        }).toSeq
    })

  // The cartesian product of the overlapping windows represents
  // a cover over the entire space. Each element of the cover is
  // a k-dim bounding box. Has size numCoverSegments containing
  // a sequence of bounds for each filter dimension, and an index
  // representing the cover key
  val segments: Seq[(Seq[NumericBoundary], CoverSegmentKey)] = Utils.cartesian(filterOverlaps)
    .zipWithIndex
    .map({ case (boundaries, key) => (boundaries, CoverSegmentKey(key)) })

  assert(segments.length == numCoverSegments)

  def withinAllBounds(bounds: Seq[NumericBoundary], filters: Vector) = bounds
    .zipWithIndex
    .forall({ case (bound, j) => bound.lower <= filters(j) && filters(j) <= bound.upper })

  def coverAssignment(filters: Vector): Seq[CoverSegmentKey] = segments
    .withFilter({ case (bounds, _) => withinAllBounds(bounds, filters) })
    .map({ case (_, key) => key })
}

