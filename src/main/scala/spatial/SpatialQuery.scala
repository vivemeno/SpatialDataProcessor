package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def Contains(rectangle:String, point:String):Boolean={
    try {
      var rect_coords = rectangle.split(",")
      var rect_x1 = rect_coords(0).trim.toDouble
      var rect_y1 = rect_coords(1).trim.toDouble
      var rect_x2 = rect_coords(2).trim.toDouble
      var rect_y2 = rect_coords(3).trim.toDouble

      var split_point = point.split(",")
      var point_x = split_point(0).trim.toDouble
      var point_y = split_point(1).trim.toDouble

      var x_min = math.min(rect_x1, rect_x2);
      var x_max = math.max(rect_x1, rect_x2);
      var y_min = math.min(rect_y1, rect_y2);
      var y_max = math.max(rect_y1, rect_y2);

      var res = false
      if (point_x >= x_min && point_x <= x_max && point_y >= y_min && point_y <= y_max)
        res = true

      return res
    }
    catch {
      case _: Throwable => return false
    }
  }

  def Within(point1:String, point2:String, distance:Double):Boolean={
    try {
      var p1_split = point1.split(",")

      var x_p1 = p1_split(0).trim.toDouble
      var y_p1 = p1_split(1).trim.toDouble

      var p2_split = point2.split(",")

      var x_p2 = p2_split(0).trim.toDouble
      var y_p2 = p2_split(1).trim.toDouble


      var dist_between_points = Math.sqrt(Math.pow((x_p2 - x_p1), 2) + Math.pow((y_p2 - y_p1), 2))

      var res = false

      if(dist_between_points <= distance)
        res = true

      return res
    }
    catch {
      case _: Throwable => return false
    }
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((true)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }


}
