package algorithms.geospatial;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import algorithms.geospatial.math.LineSegment;
import algorithms.geospatial.math.Point;
import algorithms.geospatial.math.Polygon;
import algorithms.geospatial.util.Constants;
import scala.Tuple2;

/**
 * Find the pair of points with maximum distance from given set of points
 * 
 * @author pramodh
 *
 */
public class FarthestPair {
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

		// Map input file to polygons containing a single points and filter the points with null values.
		JavaRDD<Polygon> polygon = input.map(new Function<String, Polygon>() {
			public Polygon call(String s) {
				String[] points = s.split(",");
				if (!points[0].matches("-?\\d+(\\.\\d+)?")) {
					return null;
				}
				return new Polygon(Double.parseDouble(points[0]), Double.parseDouble(points[1]));
			}
		}).filter(new Function<Polygon, Boolean>() {
			public Boolean call(Polygon v1) throws Exception {
				if (v1 == null)
					return false;
				else
					return true;
			}
		});

		// Compute the convex hull of all the points.
		Polygon convexHull = polygon.reduce(new Function2<Polygon, Polygon, Polygon>() {
			public Polygon call(Polygon arg0, Polygon arg1) throws Exception {
				return arg0.convexHull(arg1);
			}
		});

		// Create an RDD of just the vertices on the convex hull
		JavaRDD<Point> convexVertices = spark.parallelize(convexHull.getPoints());

		// Create an RDD of all possible pairs of points. Filter out pairs that have same points(since distance between them will be 0).
		JavaPairRDD<Point, Point> pairs = convexVertices.cartesian(convexVertices).filter(new Function<Tuple2<Point, Point>, Boolean>() {
			public Boolean call(Tuple2<Point, Point> v1) throws Exception {
				if (v1._1().equals(v1._2()))
					return false;
				return true;
			}
		});

		// Convert point pairs to line segments.
		JavaRDD<LineSegment> segments = pairs.map(new Function<Tuple2<Point, Point>, LineSegment>() {
			public LineSegment call(Tuple2<Point, Point> t) throws Exception {
				return new LineSegment(t._1(), t._2());
			}
		});

		// Reduce line segments to choose the one with max distance.
		LineSegment maxDistancePoints = segments.reduce(new Function2<LineSegment, LineSegment, LineSegment>() {
			public LineSegment call(LineSegment v1, LineSegment v2) throws Exception {
				if (v1.distance() > v2.distance())
					return v1;
				else
					return v2;
			}
		});

		List<LineSegment> listSeg = new ArrayList<LineSegment>();
		listSeg.add(maxDistancePoints);
		JavaRDD<LineSegment> output = spark.parallelize(listSeg);

		// Save the maximum distance points to a text file
		output.repartition(1).saveAsTextFile(args[1]);

	}

}
