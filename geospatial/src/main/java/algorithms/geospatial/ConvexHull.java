package algorithms.geospatial;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import algorithms.geospatial.math.Point;
import algorithms.geospatial.math.Polygon;
import algorithms.geospatial.util.Constants;
import algorithms.geospatial.util.SortPoints;

/**
 * Find the convex hull of a given set of points
 * 
 * @author pramodh
 *
 */
public class ConvexHull {

	/**
	 * @param args
	 *            args[0] = input file, args[1] = output file
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME);
		@SuppressWarnings("resource")
		JavaSparkContext spark = new JavaSparkContext(conf);

		JavaRDD<String> input = spark.textFile(args[0]);

		// Map input file to points and filter out null values
		JavaRDD<Point> points = input.map(new Function<String, Point>() {
			public Point call(String s) {
				String[] points = s.split(",");
				if (!points[0].matches("-?\\d+(\\.\\d+)?")) {
					return null;
				}
				return new Point(Double.parseDouble(points[0]), Double.parseDouble(points[1]));
			}
		}).filter(new Function<Point, Boolean>() {
			public Boolean call(Point p) throws Exception {
				if (p == null)
					return false;
				else
					return true;
			}
		});

		// Convert each point into a polygon having just one point
		JavaRDD<Polygon> polygon = points.map(new Function<Point, Polygon>() {
			public Polygon call(Point p) {
				ArrayList<Point> points = new ArrayList<Point>();
				points.add(p);
				return new Polygon(points);
			}
		});

		// Compute convex hull of all the polygons
		Polygon convexHull = polygon.reduce(new Function2<Polygon, Polygon, Polygon>() {
			public Polygon call(Polygon p1, Polygon p2) throws Exception {
				return p1.convexHull(p2);
			}
		});

		// Sort the vertices of convex hull points based on x and y coordinates
		JavaRDD<Point> output = spark.parallelize(SortPoints.sortPoints(convexHull.getPoints()));

		// Save vertices of convex hull into a text file
		output.repartition(1).saveAsTextFile(args[1]);
	}
}
