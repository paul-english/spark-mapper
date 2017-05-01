package mapper

import scala.collection.immutable.HashMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try

object TaxiConfig {
  val schema = StructType(Array(
    StructField("id", StringType, true),
    StructField("vendor_id", StringType, true),
    StructField("pickup_datetime", StringType, true),
    StructField("dropoff_datetime", StringType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("rate_code_id", StringType, true),
    StructField("pickup_longitude", DoubleType, true),
    StructField("pickup_latitude", DoubleType, true),
    StructField("dropoff_longitude", DoubleType, true),
    StructField("dropoff_latitude", DoubleType, true),
    StructField("passenger_count", IntegerType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("ehail_fee", DoubleType, true),
    StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true),
    StructField("payment_type", StringType, true),
    StructField("trip_type", StringType, true),
    StructField("pickup", StringType, true),
    StructField("dropoff", StringType, true),
    StructField("cab_type", StringType, true),
    StructField("precipitation", DoubleType, true),
    StructField("snow_depth", DoubleType, true),
    StructField("snowfall", DoubleType, true),
    StructField("max_temperature", DoubleType, true),
    StructField("min_temperature", DoubleType, true),
    StructField("average_wind_speed", DoubleType, true),
    StructField("pickup_nyct2010_gid", StringType, true),
    StructField("pickup_ctlabel", DoubleType, true),
    StructField("pickup_borocode", StringType, true),
    StructField("pickup_boroname", StringType, true),
    StructField("pickup_ct2010", StringType, true),
    StructField("pickup_boroct2010", StringType, true),
    StructField("pickup_cdeligibil", StringType, true),
    StructField("pickup_ntacode", StringType, true),
    StructField("pickup_ntaname", StringType, true),
    StructField("pickup_puma", StringType, true),
    StructField("dropoff_nyct2010_gid", StringType, true),
    StructField("dropoff_ctlabel", DoubleType, true),
    StructField("dropoff_borocode", StringType, true),
    StructField("dropoff_boroname", StringType, true),
    StructField("dropoff_ct2010", StringType, true),
    StructField("dropoff_boroct2010", StringType, true),
    StructField("dropoff_cdeligibil", StringType, true),
    StructField("dropoff_ntacode", StringType, true),
    StructField("dropoff_ntaname", StringType, true),
    StructField("dropoff_puma", StringType, true)
  ))

  val columnNames = HashMap(
    "_c0" -> "id",
    "_c1" -> "vendor_id",
    "_c2" -> "pickup_datetime",
    "_c3" -> "dropoff_datetime",
    "_c4" -> "store_and_fwd_flag",
    "_c5" -> "rate_code_id",
    "_c6" -> "pickup_longitude",
    "_c7" -> "pickup_latitude",
    "_c8" -> "dropoff_longitude",
    "_c9" -> "dropoff_latitude",
    "_c10" -> "passenger_count",
    "_c11" -> "trip_distance",
    "_c12" -> "fare_amount",
    "_c13" -> "extra",
    "_c14" -> "mta_tax",
    "_c15" -> "tip_amount",
    "_c16" -> "tolls_amount",
    "_c17" -> "ehail_fee",
    "_c18" -> "improvement_surcharge",
    "_c19" -> "total_amount",
    "_c20" -> "payment_type",
    "_c21" -> "trip_type",
    "_c22" -> "pickup",
    "_c23" -> "dropoff",
    "_c24" -> "cab_type",
    "_c25" -> "precipitation",
    "_c26" -> "snow_depth",
    "_c27" -> "snowfall",
    "_c28" -> "max_temperature",
    "_c29" -> "min_temperature",
    "_c30" -> "average_wind_speed",
    "_c31" -> "pickup_nyct2010_gid",
    "_c32" -> "pickup_ctlabel",
    "_c33" -> "pickup_borocode",
    "_c34" -> "pickup_boroname",
    "_c35" -> "pickup_ct2010",
    "_c36" -> "pickup_boroct2010",
    "_c37" -> "pickup_cdeligibil",
    "_c38" -> "pickup_ntacode",
    "_c39" -> "pickup_ntaname",
    "_c40" -> "pickup_puma",
    "_c41" -> "dropoff_nyct2010_gid",
    "_c42" -> "dropoff_ctlabel",
    "_c43" -> "dropoff_borocode",
    "_c44" -> "dropoff_boroname",
    "_c45" -> "dropoff_ct2010",
    "_c46" -> "dropoff_boroct2010",
    "_c47" -> "dropoff_cdeligibil",
    "_c48" -> "dropoff_ntacode",
    "_c49" -> "dropoff_ntaname",
    "_c50" -> "dropoff_puma"
  )

  val columnTypes = HashMap(
    "id" -> StringType, // 0
    "vendor_id" -> StringType, // 1
    "pickup_datetime" -> StringType, // 2
    "dropoff_datetime" -> StringType, // 3
    "store_and_fwd_flag" -> StringType, // 4
    "rate_code_id" -> StringType, // 5
    "pickup_longitude" -> DoubleType, // 6
    "pickup_latitude" -> DoubleType, // 7
    "dropoff_longitude" -> DoubleType, // 8
    "dropoff_latitude" -> DoubleType, // 9
    "passenger_count" -> IntegerType, // 10
    "trip_distance" -> DoubleType, // 11
    "fare_amount" -> DoubleType, // 12
    "extra" -> DoubleType, // 13
    "mta_tax" -> DoubleType, // 14
    "tip_amount" -> DoubleType, // 15
    "tolls_amount" -> DoubleType, // 16
    "ehail_fee" -> DoubleType, // 17
    "improvement_surcharge" -> DoubleType, // 18
    "total_amount" -> DoubleType, // 19
    "payment_type" -> StringType, // 20
    "trip_type" -> StringType, // 21
    "pickup" -> StringType, // 22
    "dropoff" -> StringType, // 23
    "cab_type" -> StringType, // 24
    "precipitation" -> DoubleType, // 25
    "snow_depth" -> DoubleType, // 26
    "snowfall" -> DoubleType, // 27
    "max_temperature" -> DoubleType, // 28
    "min_temperature" -> DoubleType, // 29
    "average_wind_speed" -> DoubleType, // 30
    "pickup_nyct2010_gid" -> StringType, // 31
    "pickup_ctlabel" -> DoubleType, // 32
    "pickup_borocode" -> StringType, // 33
    "pickup_boroname" -> StringType, // 34
    "pickup_ct2010" -> StringType, // 35
    "pickup_boroct2010" -> StringType, // 36
    "pickup_cdeligibil" -> StringType, // 37
    "pickup_ntacode" -> StringType, // 38
    "pickup_ntaname" -> StringType, // 39
    "pickup_puma" -> StringType, // 40
    "dropoff_nyct2010_gid" -> StringType, // 41
    "dropoff_ctlabel" -> DoubleType, // 42
    "dropoff_borocode" -> StringType, // 43
    "dropoff_boroname" -> StringType, // 44
    "dropoff_ct2010" -> StringType, // 45
    "dropoff_boroct2010" -> StringType, // 46
    "dropoff_cdeligibil" -> StringType, // 47
    "dropoff_ntacode" -> StringType, // 48
    "dropoff_ntaname" -> StringType, // 49
    "dropoff_puma" -> StringType // 50
  )

  val oneHotEncodeColumns = List(
    "vendor_id",
    "store_and_fwd_flag",
    "rate_code_id",
    "payment_type",
    "trip_type",
    "cab_type",
    "pickup_boroname",
    "pickup_ct2010",
    "pickup_cdeligibil",
    "pickup_ntaname",
    "pickup_puma",
    "dropoff_boroname",
    "dropoff_ct2010",
    "dropoff_cdeligibil",
    "dropoff_ntaname",
    "dropoff_puma"
  )

  val standardizeColumns = Array(
    "pickup_longitude",
    "pickup_latitude",
    "dropoff_longitude",
    "dropoff_latitude",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "ehail_fee",
    "improvement_surcharge",
    "total_amount",
    "precipitation",
    "snow_depth",
    "snowfall",
    "max_temperature",
    "min_temperature",
    "average_wind_speed",

    "pickup_dayofmonth",
    "pickup_dayofyear",
    "pickup_hour",
    "pickup_minute",
    "pickup_month",
    "pickup_quarter",
    "pickup_weekofyear",
    "pickup_year",
    "pickup_dayofweek",
    "pickup_time_cos",
    "pickup_time_sin",
    "pickup_week_cos",
    "pickup_week_sin",
    "pickup_month_cos",
    "pickup_month_sin",
    "pickup_year_cos",
    "pickup_year_sin",

    "dropoff_dayofmonth",
    "dropoff_dayofyear",
    "dropoff_hour",
    "dropoff_minute",
    "dropoff_month",
    "dropoff_quarter",
    "dropoff_weekofyear",
    "dropoff_year",
    "dropoff_dayofweek",
    "dropoff_time_cos",
    "dropoff_time_sin",
    "dropoff_week_cos",
    "dropoff_week_sin",
    "dropoff_month_cos",
    "dropoff_month_sin",
    "dropoff_year_cos",
    "dropoff_year_sin"
  )

  val int_dayofweek = udf({ (x: String) =>
    Try(x match {
      case "Sun" => 0
      case "Mon" => 1
      case "Tue" => 2
      case "Wed" => 3
      case "Thu" => 4
      case "Fri" => 5
      case "Sat" => 6
    }).toOption
  });

  val time_num = udf({ (hour: Int, minute: Int) => Try(((hour + (minute / 60.0)) / 24.0)).toOption });
  val week_num = udf({ (day: Int, hour: Int, minute: Int) => Try(((day + ((hour + (minute / 60.0)) / 24.0)) / 7)).toOption });
  val month_num = udf({ (dayofmonth: Int, last_day: Int) => Try(dayofmonth / last_day.asInstanceOf[Float]).toOption })
  val year_num = udf({ (year: Int) => Try((year / 365.0)).toOption })

  val cos2pi = udf({ (x: Float) => Try(scala.math.cos(x * 2 * scala.math.Pi)).toOption });
  val sin2pi = udf({ (x: Float) => Try(scala.math.sin(x * 2 * scala.math.Pi)).toOption });

  val is_weekend = udf({ (dayofweek: Int) =>
    Try(dayofweek match {
      case 0 => 1
      case 6 => 1
      case _ => 0
    }).toOption
  });

  val int_ampm = udf({ (x: String) =>
    Try(x match {
      case "AM" => 0
      case "PM" => 1
    }).toOption
  });

}
