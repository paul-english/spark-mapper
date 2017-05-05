package com.github.log0ymxm.mapper.clustering

case class Distribution(data: List[Double], nBins: Int) {
  val Epsilon = 0.000001
  val (max, min) = (data.max, data.min)
  val binWidth = (max - min) / nBins + Epsilon
  val bounds = (1 to nBins).map { x => min + binWidth * x }.toList

  def histo(bounds: List[Double], data: List[Double]): List[List[Double]] =
    bounds match {
      case h :: Nil => List(data)
      case h :: t => val (l, r) = data.partition(_ < h); l :: histo(t, r)
    }

  val histogram = histo(bounds, data)
}

object Cutoff {

  def histogramGap(linkage: Seq[(Int, Int, Double, Int)], diameter: Double, numBins: Integer = 10): Int = {
    if (linkage.length == 0) {
      return 1
    }

    val heights = List(0.0) ++ linkage.map(_._3) ++ List(diameter)

    val hist = Distribution(heights.toList, numBins).histogram
      .map(x => x.length)

    val firstGap = hist.zipWithIndex
      .withFilter({ case (bin, _) => bin == 0 })
      .map(_._2)
      .headOption

    val numClusters = firstGap match {
      case Some(gapIdx) => hist.drop(gapIdx).sum + 1
      case None => 1
    }

    numClusters
  }

  def firstGap(linkage: Seq[(Int, Int, Double, Int)], diameter: Double, gap: Double = 0.1): Int = {
    if (linkage.length == 0) {
      return 1
    }

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

    numClusters
  }

  def biggestGap(linkage: Seq[(Int, Int, Double, Int)], diameter: Double): Int = {
    if (linkage.length == 0) {
      return 1
    }

    val heights = linkage.map(_._3) ++ List(diameter)
    val biggestLogarithmicGap = heights.sliding(2)
      .map(x => x(1) / x(0))
      .zipWithIndex
      .maxBy(_._1)._2

    linkage.length - biggestLogarithmicGap
  }

}
