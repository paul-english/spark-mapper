package com.github.log0ymxm.mapper.clustering

object Cutoff {
  def firstGap(linkage: Seq[(Int, Int, Double, Int)], diameter: Double, gap: Double = 0.1): Int = {
    val heights = linkage.map(_._3) ++ List(diameter)
    val firstGap = heights.sliding(2)
      .map(x => x(1) - x(0))
      .zipWithIndex
      .withFilter(_._1 > gap * diameter)
      .map(_._2)
      .toList
      .headOption

    val numClusters = firstGap match {
      case Some(gapIdx) => linkage.length + 1 - gapIdx
      case None => 1
    }

    return numClusters
  }
}
