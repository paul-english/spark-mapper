package com.github.log0ymxm.mapper

import org.scalatest._

class UnionFindSpec extends FunSuite {
  test("union find") {
    val uf = new UnionFind(10)
    assert(uf.nComponents == 10)
    uf.add(1, 2)
    uf.add(2, 3)
    assert(uf.nComponents == 8)
    uf.add(1, 2)
    assert(uf.nComponents == 8)

    assert(uf.connected(1, 3) == true)
    assert(uf.connected(1, 4) == false)
  }
}
