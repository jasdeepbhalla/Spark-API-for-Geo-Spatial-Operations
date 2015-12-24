package algorithms.geospatial;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import algorithms.geospatial.math.Point;
import algorithms.geospatial.math.Polygon;
import algorithms.geospatial.util.Constants;
import scala.Tuple2;

/**
 * Find set of polygons or points that are inside given set of polygons
 * 
 * @author ankita
 *
 */
public class SpatialJoin {
	private static final String INPUT_RECT = "rectangle";
	private static final String INPUT_POINT = "point";

	/**
	 * @param args
	 *            args[0] = test data, args[1] = query rectangle file, args[2] = output file, args[3] = 'point' or 'rectangle'
	 */
	public static void main(String[] args) {
		String inputType = "rectangle";
		String outputFile = args[2];
		String testData = args[0];
		String qRect = args[1];
		inputType = args[3];
		performJoin(testData, qRect, inputType, outputFile);
	}

	/**
	 * Perform join operation based on given inputs
	 * 
	 * @param input1
	 *            The test data file location
	 * @param input2
	 *            The query rectangle file location
	 * @param inputType
	 *            Test data input type
	 * @param outputFile
	 *            Output file location
	 */
	private static void performJoin(String input1, String input2, String inputType, String outputFile) {
		SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME);
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Contents of test data file
		JavaRDD<String> fileContents = sc.textFile(input1).cache();

		// Contents of query polygon file
		JavaRDD<String> inputTestPolygons = sc.textFile(input2).cache();

		// Map entries of query rectangle into RDD of Polygon and filter out null values
		JavaRDD<Polygon> queryRect = inputTestPolygons.map(new Function<String, Polygon>() {
			public Polygon call(String s) throws Exception {
				String[] parts = s.split(",");
				if (!parts[0].matches("-?\\d+(\\.\\d+)?")) {
					return null;
				}
				long id = Long.parseLong(parts[0]);
				Point p1 = new Point(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
				Point p2 = new Point(Double.parseDouble(parts[3]), Double.parseDouble(parts[4]));
				Polygon p = new Polygon(p1, p2, id);
				return p;
			}
		}).filter(new Function<Polygon, Boolean>() {

			public Boolean call(Polygon v1) throws Exception {
				if (v1 == null)
					return false;
				else
					return true;

			}
		});

		// Perform operation based on given option
		if (inputType.equals(INPUT_RECT)) {
			performJoinOnRectangle(queryRect, fileContents, outputFile);
		} else if (inputType.equals(INPUT_POINT)) {
			performJoinOnPoints(queryRect, fileContents, outputFile);
		}

	}

	/**
	 * Perform join operation for rectangles
	 * 
	 * @param queryRect
	 *            Query rectangle file RDD
	 * @param fileContents
	 *            Input test data file RDD
	 * @param outputFile
	 *            Output file location
	 */
	private static void performJoinOnRectangle(JavaRDD<Polygon> queryRect, JavaRDD<String> fileContents, String outputFile) {
		// Convert input file to RDD of polygons and filter out null values
		JavaRDD<Polygon> polygonSet = fileContents.map(new Function<String, Polygon>() {
			public Polygon call(String s) {
				String[] parts = s.split(",");
				if (!parts[0].matches("-?\\d+(\\.\\d+)?")) {
					return null;
				}
				long id = Long.parseLong(parts[0]);
				Point p1 = new Point(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
				Point p2 = new Point(Double.parseDouble(parts[3]), Double.parseDouble(parts[4]));
				Polygon p = new Polygon(p1, p2, id);
				return p;
			}
		}).filter(new Function<Polygon, Boolean>() {
			public Boolean call(Polygon v1) throws Exception {
				if (v1 == null)
					return false;
				else
					return true;
			}
		});

		// Find polygons that join with each query polygons
		JavaPairRDD<Long, Polygon> joins = queryRect.cartesian(polygonSet).mapToPair(new PairFunction<Tuple2<Polygon, Polygon>, Long, Polygon>() {
			public Tuple2<Long, Polygon> call(Tuple2<Polygon, Polygon> arg0) throws Exception {
				Polygon set1 = arg0._1();
				Polygon set2 = arg0._2();
				if (!set1.isOverlap(set2))
					return null;
				return new Tuple2<Long, Polygon>(set1.getId(), set2);
			}
		}).filter(new Function<Tuple2<Long, Polygon>, Boolean>() {
			public Boolean call(Tuple2<Long, Polygon> arg0) throws Exception {
				if (arg0 == null)
					return false;
				else
					return true;
			}
		});

		// Group and sort by the query rectangle
		JavaPairRDD<Long, Iterable<Polygon>> result = joins.groupByKey().sortByKey();

		// Create String RDD to match output format
		JavaRDD<String> outputFormat = result.map(new Function<Tuple2<Long, Iterable<Polygon>>, String>() {
			@Override
			public String call(Tuple2<Long, Iterable<Polygon>> v1) throws Exception {
				String output = v1._1 + ",";
				Iterator itr = v1._2.iterator();
				while (itr.hasNext()) {
					Polygon element = (Polygon) itr.next();
					output += element.getId() + ",";
				}
				output = output.substring(0, output.length()-1);
				return output;
			}
		});

		// Save polygon join results into a text file
		outputFormat.repartition(1).saveAsTextFile(outputFile);
	}

	/**
	 * Perform join operation for points
	 * 
	 * @param queryRect
	 *            Query rectangle file RDD
	 * @param fileContents
	 *            Input test data file RDD
	 * @param outputFile
	 *            Output file location
	 */
	private static void performJoinOnPoints(JavaRDD<Polygon> queryRect, JavaRDD<String> fileContents, String outputFile) {
		// Convert input file to RDD of points and filter out null values
		JavaRDD<Point> points = fileContents.map(new Function<String, Point>() {
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

		// Find points that join with each query polygons
		JavaPairRDD<Long, Long> joins = queryRect.cartesian(points).mapToPair(new PairFunction<Tuple2<Polygon, Point>, Long, Long>() {
			public Tuple2<Long, Long> call(Tuple2<Polygon, Point> arg0) throws Exception {
				Polygon set = arg0._1();
				Point point = arg0._2();
				if (!set.contains(point))
					return null;
				return new Tuple2<Long, Long>(set.getId(), point.getId());
			}
		}).filter(new Function<Tuple2<Long, Long>, Boolean>() {
			public Boolean call(Tuple2<Long, Long> arg0) throws Exception {
				if (arg0 == null)
					return false;
				else
					return true;
			}
		});

		// Group and sort by the query rectangle
		JavaPairRDD<Long, Iterable<Long>> result = joins.groupByKey().sortByKey();

		// Create String RDD to match output format
		JavaRDD<String> outputFormat = result.map(new Function<Tuple2<Long, Iterable<Long>>, String>() {
			@Override
			public String call(Tuple2<Long, Iterable<Long>> v1) throws Exception {
				String output = v1._1 + ",";
				Iterator itr = v1._2.iterator();
				while (itr.hasNext()) {
					Long element = (Long) itr.next();
					output = output + element + ",";
				}
				output = output.substring(0, output.length()-1);
				return output;
			}
		});

		// Save point join results into a text file
		outputFormat.repartition(1).saveAsTextFile(outputFile);
	}

}
