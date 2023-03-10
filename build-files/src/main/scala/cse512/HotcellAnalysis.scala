package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("pickupInfo")
  val reqP = spark.sql("select x,y,z,count(*) as countVal from pickupInfo where x>=" + minX + " and x<=" + maxX + " and y>="+minY +" and y<="+maxY+" and z>="+minZ+" and z<=" +maxZ +" group by x,y,z").persist()
  reqP.createOrReplaceTempView("reqP")    
    
  val p = spark.sql("select sum(countVal) as total_sum, sum(countVal*countVal) as sumSqr from reqP").persist()
  val total_sum = p.first().getLong(0).toDouble
  val sumSqr = p.first().getLong(1).toDouble  
  
  val mean = (total_sum/numCells)
  val std_dev = Math.sqrt((sumSqr/numCells) - (mean*mean))   
  
  val neighbour = spark.sql("select group1.x as x , group1.y as y, group1.z as z, count(*) as num_neighbours, sum(group2.countVal) as sigma from reqP as group1 inner join reqP as group2 on ((abs(group1.x-group2.x) <= 1 and  abs(group1.y-group2.y) <= 1 and abs(group1.z-group2.z) <= 1)) group by group1.x, group1.y, group1.z").persist()
  neighbour.createOrReplaceTempView("neighbour")
  
  spark.udf.register("zScore",(mean: Double, std_dev:Double, num_neighbours: Int, sigma: Int, numCells:Int)=>((
    HotcellUtils.zScore(mean, std_dev, num_neighbours, sigma, numCells)
    )))  
  
  val withZscore =  spark.sql("select x,y,z,zScore("+ mean + ","+ std_dev +",num_neighbours,sigma," + numCells+") as zScore from neighbour")
  withZscore.createOrReplaceTempView("withZscore")
  
  val retVal = spark.sql("select x,y,z from withZscore order by zScore desc")
  return retVal
  // YOU NEED TO CHANGE THIS PART
}
}
