package algorithms.geospatial;

import java.io.Serializable;

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
 * Find the union of a given set of polygons
 * 
 * @author pramodh
 *
 */
@SuppressWarnings("serial")
public class GeometricUnion implements Serializable {
	/**
	 * @param args
	 *            args[0] = input file, args[1] = output file
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME);
		@SuppressWarnings("resource")
		JavaSparkContext spark = new JavaSparkContext(conf);

		JavaRDD<String> input = spark.textFile(args[0]);

		// Map input to polygons and filter the polygons with null values
		JavaRDD<Polygon> polygon = input.map(new Function<String, Polygon>() {
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

		// Reduce as union of polygons
		Polygon union = polygon.reduce(new Function2<Polygon, Polygon, Polygon>() {
			public Polygon call(Polygon v1, Polygon v2) throws Exception {
				return v1.union(v2);
			}
		});

		// Sort the vertices of union polygon based on x and y coordinates
		JavaRDD<Point> output = spark.parallelize(SortPoints.sortPoints(union.getPoints()));

		// Save the vertices of union polygon into a text file
		output.repartition(1).saveAsTextFile(args[1]);

	}

}
