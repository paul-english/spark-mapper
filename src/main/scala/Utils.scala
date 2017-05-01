package mapper.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vector, DenseVector, SparseVector, Vectors }
import breeze.linalg.{ DenseVector => BDV, SparseVector => BSV, Vector => BV }
import org.apache.spark.mllib.linalg.distributed.{ IndexedRowMatrix, IndexedRow, BlockMatrix }

object Utils {
  def toBlockMatrix(x: RDD[Vector], rowsPerBlock: Int = 1024, colsPerBlock: Int = 1024): BlockMatrix = {
    new IndexedRowMatrix(
      x.zipWithIndex().map({ xi => IndexedRow(xi._2, xi._1) })
    ).toBlockMatrix(rowsPerBlock, colsPerBlock)
  }

  def toBreeze(v: Vector): BV[Double] = v match {
    case DenseVector(values) => new BDV[Double](values)
    case SparseVector(size, indices, values) => new BSV[Double](indices, values, size)
  }

  def toSpark(bv: BV[Double]): Vector = bv match {
    case v: BDV[Double] => new DenseVector(v.toArray)
    case v: BSV[Double] => new SparseVector(v.length, v.index, v.data)
  }

  def cartesian[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq.empty[A])) { (x, y) => for (a <- x; b <- y) yield a :+ b }

}
