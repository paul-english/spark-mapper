package com.github.log0ymxm.mapper.examples.mnist

import java.io.FileOutputStream
import java.net.URL
import java.nio.channels.Channels
import java.nio.file.{ Files, Paths }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MNISTData {
  def fetchMnist(sc: SparkContext, path: String, name: String): RDD[breeze.linalg.DenseVector[Double]] = {
    val location = s"$path/$name-mnist-dense-with-labels.data"

    if (!Files.exists(Paths.get(location))) {
      val url = s"http://mnist-data.s3.amazonaws.com/$name-mnist-dense-with-labels.data"
      val channel = Channels.newChannel(new URL(url).openStream())
      val fos = new FileOutputStream(location)
      fos.getChannel.transferFrom(channel, 0, Long.MaxValue)
    }

    sc.textFile(location).map(row => breeze.linalg.DenseVector(row.split(",").map(_.toDouble)))
  }
}
