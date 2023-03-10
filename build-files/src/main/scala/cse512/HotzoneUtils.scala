package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
	
	val rect_coord = queryRectangle.split(",")
	val point_coord = pointString.split(",")

	val pt_x: Double = point_coord(0).trim.toDouble
	val pt_y: Double = point_coord(1).trim.toDouble
	val rect_x1: Double = math.min(rect_coord(0).trim.toDouble, rect_coord(2).trim.toDouble)
	val rect_y1: Double = math.min(rect_coord(1).trim.toDouble, rect_coord(3).trim.toDouble)
	val rect_x2: Double = math.max(rect_coord(0).trim.toDouble, rect_coord(2).trim.toDouble)
	val rect_y2: Double = math.max(rect_coord(1).trim.toDouble, rect_coord(3).trim.toDouble)

	if ((pt_x >= rect_x1) && (pt_x <= rect_x2) && (pt_y >= rect_y1) && (pt_y <= rect_y2)) {
		return true
	}
	return false
  }
}
