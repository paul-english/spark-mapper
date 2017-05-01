package com.github.log0ymxm.mapper

import java.io.{ BufferedWriter, File, FileWriter }

import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, IndexedRow, IndexedRowMatrix }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{ Edge, Graph }

import com.github.log0ymxm.mapper.clustering.{ SingleLinkage, Cutoff }

object Mapper {

  /**
   * Computes 1-dimensional simplicial complex from a dataset represented by
   * it's pairwise distances and a filtration.
   *
   * @param sc A SparkContext
   * @param distances An n x n upper triangular matrix of pairwise distances,
   * @param filterValues An n x k matrix, representing the k filter
   *                     functions that have been applied to the original
   *                     data points. Indices should match up with the
   *                     coordinates found in the distances matrix.
   * @return GraphX structure representing the reduced dimension simplicial complex
   */
  def mapper(sc: SparkContext, distances: CoordinateMatrix, filterValues: IndexedRowMatrix, coverIntervals: Int = 10, coverOverlapRatio: Double = 0.5): Graph[String, Int] = {
    val n = distances.numRows().toInt;

    val cover = new Cover(filterValues, coverIntervals, coverOverlapRatio)

    // combine rows and columns of distance matrix since we only have it in upper triangular form.
    // This has size n x n since for all elements n, we have the n pairwise distances
    val pairwiseDistances: RDD[(DataKey, PointDistance)] = distances.entries
      .union(distances.transpose().entries)
      .map((entry) => { (DataKey(entry.i), PointDistance(DataKey(entry.j), entry.value)) })

    // filter indices match up with the keys in our distances
    // RDD. We cogroup them and will ensure
    val filterDistances: RDD[(DataKey, (IndexedRow, Iterable[PointDistance]))] = filterValues
      .rows
      .map({ idxRow => (DataKey(idxRow.index), idxRow) })
      .cogroup(pairwiseDistances)
      .flatMapValues({ case (f, d) => f.map({ x => (x, d) }) })

    // for each data point, we only return the cover element keys
    // that this data point belongs too, i.e. the data point
    // filter values are within the cover bounding box for all
    // k dimensions
    val dataDistancesAndCoverAssignment: RDD[(DataKey, (IndexedRow, Iterable[PointDistance], Seq[CoverSegmentKey]))] = filterDistances
      .map({
        case (dataKey, (filterRow, rowDistances)) =>
          val coverAssignment: Seq[CoverSegmentKey] = cover.coverAssignment(filterRow.vector)
          (dataKey, (filterRow, rowDistances, coverAssignment))
      })

    // key each element in the data by which patch it should be in,
    // duplicates rows if they're in multiple patches. This is
    // longer than the initial dataset, since rows are duplicated
    // anywhere we have a point in multiple cover segments.
    val flattenedDataWithCoverKey: RDD[(CoverSegmentKey, (DataKey, IndexedRow, Iterable[PointDistance]))] = dataDistancesAndCoverAssignment
      .flatMap({
        case (dataKey, (filterRow, distances, coverAssignment)) =>
          coverAssignment.map({ assignment => (assignment, (dataKey, filterRow, distances)) })
      })

    val partitionedData: RDD[(CoverSegmentKey, (DataKey, IndexedRow, Iterable[PointDistance]))] = flattenedDataWithCoverKey
      .partitionBy(new CoverAssignmentPartitioner(cover.numCoverSegments))

    val clusters: RDD[(DataKey, String)] = partitionedData.mapPartitions({
      case (patch: Iterator[(CoverSegmentKey, (DataKey, IndexedRow, Iterable[PointDistance]))]) =>
        val (keys, elements) = patch.toList.unzip
        val segmentKey = keys(0)
        val (indexKeys, filterValues, distances) = elements.unzip3
        val n = elements.length

        val localDistances: DenseMatrix[Double] = new DenseMatrix(n, n, elements.flatMap({
          case (currentIndex, _, pointDistances) =>
            indexKeys collect {
              case i if i == currentIndex => 0
              case i if i != currentIndex =>
                pointDistances.filter({ d => d.coordinate == i }).head.distance
            }
        }).toArray)
        val diameter = localDistances.max
        val linkage = SingleLinkage(localDistances)

        val numClusters = Cutoff.firstGap(linkage, diameter)

        val clusters = SingleLinkage.fcluster(linkage, numClusters)
        val clusterNames = clusters.map(x => s"${segmentKey.id}-$x")
        // TODO add in the average value of the segment
        // TODO add in the number of nodes in this cluster
        indexKeys.zip(clusterNames).toIterator
    })

    // TODO what other props should be added to the graph vertices?
    val vertices: RDD[(Long, (String))] = clusters
      .map(_._2)
      .distinct
      .zipWithIndex
      .map({ case (name, idx) => (idx, (name)) })

    val idLookup = sc.broadcast(
      vertices
      .map({ case (id, (name)) => (name, id) })
      .collect()
      .toMap
    )

    val assignments: RDD[(DataKey, CoverSegmentKey)] = dataDistancesAndCoverAssignment.flatMapValues(_._3)

    val edges: RDD[Edge[Int]] = clusters.cogroup(assignments)
      .flatMap({
        case (key: DataKey, (vertices: Seq[String], segments: Seq[CoverSegmentKey])) =>
          val weight = segments.toSeq.length
          vertices.toSeq.combinations(2).map({ x =>
            val node1 = idLookup.value(x(0))
            val node2 = idLookup.value(x(1))
            ((node1, node2), 1)
          })
      })
      .reduceByKey({ case (x: Int, y: Int) => x + y })
      .map({ case ((n1, n2), w) => Edge(n1, n2, w) })

    val graph: Graph[String, Int] = Graph(vertices, edges)

    return graph
  }

  /**
   * If you expect your resultant graph structure to fit in memory, this will
   * serialize your simplicial complex into a json structure suitable for
   * visualization.
   *
   * @param graph Simplicial complex result from mapper algorithm
   * @param graphPath Location where json file should be written
   */
  def writeAsJson(graph: Graph[String, Int], graphPath: String) = {
    val vertices = graph.vertices.map(v => Map(
      "id" -> v._1,
      "name" -> v._2
    )).collect()
    val edges = graph.edges.map({
      case Edge(src, dst, weight) =>
        Map(
          "src" -> src,
          "dst" -> dst,
          "weight" -> weight
        )
    }).collect()

    val json = JsonUtil.toJson(Map(
      "vertices" -> vertices,
      "edges" -> edges
    ))

    val bw = new BufferedWriter(new FileWriter(new File(graphPath)))
    bw.write(json)
    bw.close()
  }

}
