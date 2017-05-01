package com.github.log0ymxm.mapper.clustering

import breeze.linalg.DenseMatrix

import scala.collection.mutable.{ ListBuffer, PriorityQueue }
import scala.math.Ordering

import com.github.log0ymxm.mapper.UnionFind

object SingleLinkage {
  private val ord = new Ordering[(Int, Int, Double)] {
    def compare(x: (Int, Int, Double), y: (Int, Int, Double)): Int = y._3 compare x._3
  }

  /**
   * Obtain a linkage matrix using Kruskal's algorithm.
   *
   * @param distances Pairwise distance matrix
   * @return Linkage matrix as a sequence of tuples that represent dendrogram. (left index, right index, distance between elements, new cluster size)
   */
  def apply(distances: DenseMatrix[Double]): Seq[(Int, Int, Double, Int)] = {
    val n = distances.rows
    val edges = new PriorityQueue[(Int, Int, Double)]()(ord)
    val mst = new ListBuffer[(Int, Int, Double, Int)]
    val uf = new UnionFind(n)
    var nAdded = 0

    edges ++= (0 until n)
      .combinations(2)
      .map({ (ab: Seq[Int]) =>
        (ab(0), ab(1), distances(ab(0), ab(1)))
      })

    val cluster = Array.fill[Int](n)(-1)

    while (!edges.isEmpty && (nAdded < n - 1)) {
      val (u, v, distance) = edges.dequeue
      if (!uf.connected(u, v)) {
        val clusterSize = uf.add(u, v)
        mst += ((u, v, distance, clusterSize))
        nAdded += 1
      }
    }

    mst
  }

  /**
   * Given a linkage matrix return a sequence that represents the clusters
   * each index is assigned too.
   *
   * @param linkages Linkage matrix result from SingleLinkage
   * @param numClusters Number of unique clusters to group elements into
   * @return Sequence with the cluster assignment for each index
   */
  def fcluster(linkages: Seq[(Int, Int, Double, Int)], numClusters: Int): Seq[Int] = {
    val n = linkages.length + 1

    // early exit easy case
    if (numClusters == 1) {
      Array.fill[Int](n)(0).toSeq
    } else {
      val z: Seq[(Int, Int)] = linkages.take(n - numClusters).map(x => (x._1, x._2))

      val uf = new UnionFind(n)
      z.foreach { case (a, b) => uf.add(a, b) }

      val parents = (0 until n).map(x => uf.find(x))
      val uniqueParents = parents.distinct
      val clusters = parents.map(x => uniqueParents.indexOf(x))

      clusters
    }
  }
}

