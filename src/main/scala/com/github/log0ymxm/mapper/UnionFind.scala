package com.github.log0ymxm.mapper

class UnionFind(val n: Int) {
  private[this] val comp = (0 until n).toArray[Int]
  private[this] val compSize = Array.fill[Int](n)(1)
  private[this] var nComp: Int = n

  def add(u: Int, v: Int): Int = {
    val uRoot = find(u)
    val vRoot = find(v)

    if (uRoot != vRoot) {
      if (compSize(uRoot) < compSize(vRoot)) {
        comp(uRoot) = vRoot
        compSize(vRoot) += compSize(uRoot)
      } else {
        comp(vRoot) = uRoot
        compSize(uRoot) += compSize(vRoot)
      }
      nComp -= 1
    }
    return compSize(find(u))
  }

  def find(u: Int): Int = {
    if (u == comp(u)) {
      u
    } else {
      var compIdx = u
      while (compIdx != comp(compIdx)) {
        comp(compIdx) = comp(comp(compIdx))
        compIdx = comp(compIdx)
      }
      compIdx
    }
  }

  def nComponents: Int = nComp

  def connected(u: Int, v: Int): Boolean = find(u) == find(v)
}
