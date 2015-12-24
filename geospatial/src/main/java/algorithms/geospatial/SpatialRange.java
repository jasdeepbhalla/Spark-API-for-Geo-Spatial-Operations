package algorithms.geospatial;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import algorithms.geospatial.math.Point;
import algorithms.geospatial.math.Polygon;
import algorithms.geospatial.util.Constants;

/**
 * Perform spatial range on given set of points using a query rectangle
 * 
 * @author ankita
 *
 */
public class SpatialRange {
	/**
	 * @param args
	 *            args[0] = test input file, args[1] = query rectangle file, args[2] = output file
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME);
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile(args[0]);

		// Map input file to RDD of points and filter out null values
		JavaRDD<Point> points = input.map(new Function<String, Point>() {
			public Point call(String s) throws Exception {
				String[] points = s.split(",");
				if (!points[0].matches("-?\\d+(\\.\\d+)?")) {
					return null;
				}
				long id = Long.parseLong(points[0]);
				double x1 = Double.parseDouble(points[1]);
				double y1 = Double.parseDouble(points[2]);
				return new Point(x1, y1, id);
			}
		}).filter(new Function<Point, Boolean>() {
			public Boolean call(Point v1) throws Exception {
				if (v1 == null)
					return false;
				else
					return true;
			}
		});

		JavaRDD<String> inputRect = sc.textFile(args[1]);

		// Map query rectangle file to RDD of polygon and filter out null values
		JavaRDD<Polygon> queryPolygon = inputRect.map(new Function<String, Polygon>() {
			public Polygon call(String s) throws Exception {
				String[] points = s.split(",");
				if (!points[0].matches("-?\\d+(\\.\\d+)?")) {
					return null;
				}
				double x1 = Double.parseDouble(points[0]);
				double y1 = Double.parseDouble(points[1]);
				double x2 = Double.parseDouble(points[2]);
				double y2 = Double.parseDouble(points[3]);
				return new Polygon(new Point(x1, y1), new Point(x2, y2), 0);
			}
		}).filter(new Function<Polygon, Boolean>() {
			public Boolean call(Polygon v1) throws Exception {
				if (v1 == null)
					return false;
				else
					return true;
			}
		});

		// Get the first rectangle as input data will have single query rectangle
		final Polygon queryRect = queryPolygon.first();

		// Get RDD of points that fall in range of query rectangle and sort them based on ID
		JavaRDD<Point> rangePoints = points.filter(new Function<Point, Boolean>() {
			public Boolean call(Point point) throws Exception {
				return queryRect.contains(point);
			}
		}).sortBy(new Function<Point, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Point value) throws Exception {
				return value.getId();
			}
		}, true, 1);

		// Create RDD of point IDs from created range points
		JavaRDD<Long> pointIds = rangePoints.map(new Function<Point, Long>() {
			@Override
			public Long call(Point p) throws Exception {
				return p.getId();
			}
		});

		// Save the points that fall in range of query rectange into a text file
		pointIds.repartition(1).saveAsTextFile(args[2]);
	}
}
