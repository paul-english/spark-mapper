package mapper

import org.apache.log4j.{ LogManager, Level }
import org.apache.spark.ml
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{ OneHotEncoder, StringIndexer, StandardScaler, VectorAssembler }
import org.apache.spark.mllib
import org.apache.spark.mllib.linalg.distributed.{ IndexedRowMatrix, IndexedRow, CoordinateMatrix, MatrixEntry }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.util.Try

object TaxiDriver {
  // Converts from an ml Vector to an mllib Vector to be used with coordinate matrix
  def objToVector(features: Object): mllib.linalg.Vector = {
    println(s"features $features")
    val dense: ml.linalg.DenseVector = features match {
      case f: ml.linalg.DenseVector => f
      case sparse: ml.linalg.SparseVector => sparse.toDense
    }
    println(s"dense $dense")

    mllib.linalg.Vectors.dense(dense.toArray)
  }

  def main(args: Array[String]) {
    LogManager.getLogger("org").setLevel(Level.WARN)

    val builder = SparkSession.builder()
      .appName("Taxi Mapper")
    val spark = builder.getOrCreate()

    val df = spark.read
      .option("header", "false")
      .schema(TaxiConfig.schema)
      .csv("s3n://frst-nyc-data/trips/trips_*.csv.gz")
      .repartition(3000)
      .cache

    df.show

    println(df.stat.freqItems(Array("vendor_id", "store_and_fwd_flag")))

    val taxiDf = df
      .withColumn("pickup_timestamp", unix_timestamp(col("pickup_datetime")))
      .withColumn("pickup_dayofmonth", dayofmonth(col("pickup_datetime")))
      .withColumn("pickup_dayofyear", dayofyear(col("pickup_datetime")))
      .withColumn("pickup_hour", hour(col("pickup_datetime")))
      .withColumn("pickup_minute", minute(col("pickup_datetime")))
      .withColumn("pickup_month", month(col("pickup_datetime")))
      .withColumn("pickup_quarter", quarter(col("pickup_datetime")))
      .withColumn("pickup_weekofyear", weekofyear(col("pickup_datetime")))
      .withColumn("pickup_year", year(col("pickup_datetime")))
      .withColumn("pickup_dayofweek", TaxiConfig.int_dayofweek(date_format(col("pickup_datetime"), "E")))
      .withColumn("pickup_time_num", TaxiConfig.time_num(col("pickup_hour"), col("pickup_minute")))
      .withColumn("pickup_time_cos", TaxiConfig.cos2pi(col("pickup_time_num")))
      .withColumn("pickup_time_sin", TaxiConfig.sin2pi(col("pickup_time_num")))
      .withColumn("pickup_week_num", TaxiConfig.week_num(col("pickup_dayofweek"), col("pickup_hour"), col("pickup_minute")))
      .withColumn("pickup_week_cos", TaxiConfig.cos2pi(col("pickup_week_num")))
      .withColumn("pickup_week_sin", TaxiConfig.sin2pi(col("pickup_week_num")))
      .withColumn("pickup_month_lastday", dayofmonth(last_day(col("pickup_datetime"))))
      .withColumn("pickup_month_cos", TaxiConfig.cos2pi(TaxiConfig.month_num(col("pickup_month"), col("pickup_month_lastday"))))
      .withColumn("pickup_month_sin", TaxiConfig.sin2pi(TaxiConfig.month_num(col("pickup_month"), col("pickup_month_lastday"))))
      .withColumn("pickup_year_cos", TaxiConfig.cos2pi(TaxiConfig.year_num(col("pickup_year"))))
      .withColumn("pickup_year_sin", TaxiConfig.sin2pi(TaxiConfig.year_num(col("pickup_year"))))
      .withColumn("pickup_isweekend", TaxiConfig.is_weekend(col("pickup_dayofweek")))
      .withColumn("pickup_ispm", TaxiConfig.int_ampm(date_format(col("pickup_datetime"), "a")))
      .withColumn("dropoff_timestamp", unix_timestamp(col("dropoff_datetime")))
      .withColumn("dropoff_dayofmonth", dayofmonth(col("dropoff_datetime")))
      .withColumn("dropoff_dayofyear", dayofyear(col("dropoff_datetime")))
      .withColumn("dropoff_hour", hour(col("dropoff_datetime")))
      .withColumn("dropoff_minute", minute(col("dropoff_datetime")))
      .withColumn("dropoff_month", month(col("dropoff_datetime")))
      .withColumn("dropoff_quarter", quarter(col("dropoff_datetime")))
      .withColumn("dropoff_weekofyear", weekofyear(col("dropoff_datetime")))
      .withColumn("dropoff_year", year(col("dropoff_datetime")))
      .withColumn("dropoff_dayofweek", TaxiConfig.int_dayofweek(date_format(col("dropoff_datetime"), "E")))
      .withColumn("dropoff_time_num", TaxiConfig.time_num(col("dropoff_hour"), col("dropoff_minute")))
      .withColumn("dropoff_time_cos", TaxiConfig.cos2pi(col("dropoff_time_num")))
      .withColumn("dropoff_time_sin", TaxiConfig.sin2pi(col("dropoff_time_num")))
      .withColumn("dropoff_week_num", TaxiConfig.week_num(col("dropoff_dayofweek"), col("dropoff_hour"), col("dropoff_minute")))
      .withColumn("dropoff_week_cos", TaxiConfig.cos2pi(col("dropoff_week_num")))
      .withColumn("dropoff_week_sin", TaxiConfig.sin2pi(col("dropoff_week_num")))
      .withColumn("dropoff_month_lastday", dayofmonth(last_day(col("dropoff_datetime"))))
      .withColumn("dropoff_month_cos", TaxiConfig.cos2pi(TaxiConfig.month_num(col("dropoff_month"), col("dropoff_month_lastday"))))
      .withColumn("dropoff_month_sin", TaxiConfig.sin2pi(TaxiConfig.month_num(col("dropoff_month"), col("dropoff_month_lastday"))))
      .withColumn("dropoff_year_cos", TaxiConfig.cos2pi(TaxiConfig.year_num(col("dropoff_year"))))
      .withColumn("dropoff_year_sin", TaxiConfig.sin2pi(TaxiConfig.year_num(col("dropoff_year"))))
      .withColumn("dropoff_isweekend", TaxiConfig.is_weekend(col("dropoff_dayofweek")))
      .withColumn("dropoff_ispm", TaxiConfig.int_ampm(date_format(col("dropoff_datetime"), "a")))

    taxiDf.printSchema

    taxiDf.show

    println(s"--- approx count ${taxiDf.rdd.countApprox(100000)}")

    val nonnullDf = taxiDf.na.fill("unknown", TaxiConfig.oneHotEncodeColumns)
      .na.fill(0, List(
        "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount",
        "precipitation", "snow_depth", "snowfall"
      ))
      .na.drop()
      .cache

    println(s"---- nonull count ${nonnullDf.rdd.count()}")
    //val sampleDF = nonnullDf.sample(true, 0.001).cache

    val indexers = TaxiConfig.oneHotEncodeColumns.map { c => (c, new StringIndexer().setInputCol(c).setOutputCol(s"${c}_index")) }
    val oneHotEncoders = TaxiConfig.oneHotEncodeColumns.map { c => (c, new OneHotEncoder().setInputCol(s"${c}_index").setOutputCol(s"${c}_onehot")) }

    val assembler = new VectorAssembler()
      .setInputCols(TaxiConfig.oneHotEncodeColumns.map(x => s"${x}_onehot").toArray ++ TaxiConfig.standardizeColumns)
      .setOutputCol("assembled")

    val scaler = new StandardScaler()
      .setInputCol("assembled")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    val pipeStages = (
      (indexers.map(_._2) ++ oneHotEncoders.map(_._2))
      ++ Array(assembler, scaler)
    ).toArray

    val pipeline = new Pipeline().setStages(pipeStages)

    val model = pipeline.fit(nonnullDf)

    val transformedDf = model.transform(nonnullDf).select("id", "features").cache

    transformedDf.show(2)

    val matrix = new IndexedRowMatrix(transformedDf.select("id", "features").rdd.map {
      case Row(id: Int, features: Object) =>
        IndexedRow(id, objToVector(features))
    })

    val similarities = matrix.toCoordinateMatrix
      .transpose()
      .toIndexedRowMatrix()
      .columnSimilarities()

    val dist = new CoordinateMatrix(
      similarities
        .entries
        .map((entry) => new MatrixEntry(entry.i, entry.j, 1 - entry.value))
    )

    println("Running Mapper")
    val graph = Mapper.mapper(
      sc,
      dist,
      filtered,
      partitionSize
    )

    Mapper.writeAsJson(graph, "s3n://frst-nyc-data/graph.json")

    sc.stop()

  }
}
