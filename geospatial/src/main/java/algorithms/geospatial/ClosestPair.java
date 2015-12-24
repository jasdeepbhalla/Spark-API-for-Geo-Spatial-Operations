package algorithms.geospatial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import algorithms.geospatial.math.LineSegment;
import algorithms.geospatial.math.Point;
import algorithms.geospatial.util.Constants;
import algorithms.geospatial.util.SortPoints;

/**
 * Find closest pair of points from a given set of points
 * 
 * @author ankita
 *
 */
public class ClosestPair implements Serializable {

	/**
	 * @param args
	 *            args[0] = input file, args[1] = output file
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME);
		@SuppressWarnings("resource")
		JavaSparkContext spark = new JavaSparkContext(conf);

		JavaRDD<String> input = spark.textFile(args[0]);

		// Map input file to points and filter the polygons with null values
		JavaRDD<Point> points = input.map(new Function<String, Point>() {
			public Point call(String s) {
				String[] points = s.split(",");
				if (!points[0].matches("-?\\d+(\\.\\d+)?")) {
					return null;
				}
				return new Point(Double.parseDouble(points[0]), Double.parseDouble(points[1]));
			}
		}).filter(new Function<Point, Boolean>() {
			public Boolean call(Point v1) throws Exception {
				if (v1 == null)
					return false;
				else
					return true;
			}
		});

		// Create a broadcast list of all points
		final Broadcast<List<Point>> allPoints = spark.broadcast(points.take((int) points.count()));

		// Create RDD of line segments of each point with its closest point
		JavaRDD<LineSegment> minDistPair = points.map(new Function<Point, LineSegment>() {
			@Override
			public LineSegment call(Point point) {
				double maxDist = Double.MAX_VALUE;
				double mindist = Double.MIN_VALUE;
				Point otherPoint = null;
				for (int i = 0; i < allPoints.getValue().size(); i++) {
					Point p = allPoints.getValue().get(i);
					if (!p.equals(point)) {
						mindist = new LineSegment(p, point).distance();
						if (mindist < maxDist && mindist != 0.0) {
							maxDist = mindist;
							otherPoint = allPoints.getValue().get(i);
						}
					}
				}
				LineSegment lineSeg = new LineSegment(point, otherPoint);
				return lineSeg;
			}
		});

		// Sort all the line segments based on their x and y coordinates
		ArrayList<LineSegment> segments = SortPoints.sortLineSegment(minDistPair.collect());

		// The distance between points having minimum distance
		final double minDist = segments.get(0).distance();

		// Get all the line segments that have same minimum distance
		JavaRDD<LineSegment> minSameDist = minDistPair.filter(new Function<LineSegment, Boolean>() {
			@Override
			public Boolean call(LineSegment arg0) throws Exception {
				if (arg0.distance() == minDist) {
					return true;
				} else
					return false;
			}
		});

		segments.clear();
		segments.add(minSameDist.first());

		// Save the minimum distance points to a text file
		spark.parallelize(segments).repartition(1).saveAsTextFile(args[1]);
	}

}
