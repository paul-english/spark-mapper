package com.github.log0ymxm.mapper.examples.mnist

import org.apache.log4j.{ Level, LogManager }
import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry }
import org.apache.spark.mllib.linalg.{ DenseVector, Vector, Vectors }
import org.apache.spark.{ SparkConf, SparkContext }

import com.github.log0ymxm.mapper.Mapper

object MNISTDriver {

  def main(args: Array[String]) {
    LogManager.getLogger("org").setLevel(Level.WARN)

    val local = args contains "--local"

    val partitionSize = 500

    val conf = new SparkConf()
      .setAppName("Mnist Mapper")
    if (local) {
      conf.setMaster("local[6]")
    }
    val sc = new SparkContext(conf)

    val mnistTrain = if (local) {
      MNISTData.fetchMnist(sc, "/Users/penglish", "train")
        .repartition(partitionSize)
    } else {
      sc.textFile("s3n://frst-nyc-data/train-mnist-dense-with-labels.data")
        .map(row => breeze.linalg.DenseVector(row.split(",").map(_.toDouble)))
        .repartition(partitionSize)
    }
    println(s"--- mnistTrain num partitions ${mnistTrain.partitions.size}")

    println("Removing labels")
    val noLabels = mnistTrain.map({ x => x(1 until x.length) })

    println("Converting to mllib DenseVectors")
    val trainImages = noLabels.map({ x =>
      val v: Vector = new DenseVector(x.toArray)
      v
    })

    val useSample = args contains "--sample"
    val workingData = useSample match {
      case true => {
        println(s"Sampling MNIST data")
        trainImages.sample(true, 0.01)
      }
      case false => trainImages
    }

    println("Adding index")
    val indexedRDD = workingData.zipWithIndex.map {
      case (value, index) => IndexedRow(index, value)
    }
    println("Converting to indexed matrix")
    val matrix = new IndexedRowMatrix(indexedRDD)
    println("Calculating Similarities")
    val similarities = matrix.toCoordinateMatrix
      .transpose()
      .toIndexedRowMatrix()
      .columnSimilarities()
    println("Converting to distances")
    val dist = new CoordinateMatrix(
      similarities
        .entries
        .map((entry) => new MatrixEntry(entry.i, entry.j, 1 - entry.value))
    )

    // TODO graph laplacian on mnist
    // tangent metric
    println("Calculating filtration")
    val filtered = new IndexedRowMatrix(indexedRDD.map({
      case (imageRow) =>
        IndexedRow(imageRow.index, new DenseVector(Array(
          // testing on just one filter function currently
          Vectors.norm(imageRow.vector, 2)
        )))
    }))

    println(s"MNIST Count: ${workingData.count()}")

    println("Running Mapper")
    val graph = Mapper.mapper(
      sc,
      dist,
      filtered,
      partitionSize
    )

    if (local) {
      Mapper.writeAsJson(graph, "mnist-graph.json")
    } else {
      Mapper.writeAsJson(graph, "s3n://frst-nyc-data/graph.json")
    }

    sc.stop()
  }
}
