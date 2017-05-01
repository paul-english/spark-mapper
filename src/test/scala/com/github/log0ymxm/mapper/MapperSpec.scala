package com.github.log0ymxm.mapper

import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry }
import org.apache.spark.mllib.linalg.{ DenseVector, Vector, Vectors }

class MapperSpec extends FunSuite with SharedSparkContext {

  test("simple mapper on noisy circle") {
    val spark = SparkSession.builder().getOrCreate()

    val fileLoc = getClass.getClassLoader.getResource("circle.csv").getPath()
    val circle = spark.read
      .option("header", false)
      .option("inferSchema", true)
      .csv(fileLoc)

    assert(circle.count == 300)

    val indexedRDD = circle.rdd.zipWithIndex.map {
      case (Row(x: Double, y: Double), i) =>
        val v: Vector = new DenseVector(Array(x + 2, y + 2))
        IndexedRow(i, v)
    }
    val matrix = new IndexedRowMatrix(indexedRDD)
    val similarities = matrix.toCoordinateMatrix
      .transpose()
      .toIndexedRowMatrix()
      .columnSimilarities()
    val distances = new CoordinateMatrix(
      similarities
        .entries
        .map((entry) => new MatrixEntry(entry.i, entry.j, 1 - entry.value))
    )

    val filtration = new IndexedRowMatrix(indexedRDD.map({ row =>
      IndexedRow(row.index, new DenseVector(Array(Vectors.norm(row.vector, 2))))
    }))

    val graph = Mapper.mapper(sc, distances, filtration, 10, 0.5)
    //Mapper.writeAsJson(graph, "mapper-vis/circle-graph.json")

    assert(graph.vertices.count == 28)
    assert(graph.edges.count == 25)
  }
}
