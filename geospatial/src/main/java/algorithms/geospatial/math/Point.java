package algorithms.geospatial.math;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Class to represent a point
 * 
 * @author pramodh
 *
 */
@SuppressWarnings("serial")
public class Point implements Serializable {

	/**
	 * Coordinates
	 */
	private double x, y;
	private long id;

	@Override
	public boolean equals(Object anotherPoint) {
		if (anotherPoint instanceof Point) {
			Point p = (Point) anotherPoint;
			if (this.x == p.getX() && this.y == p.getY())
				return true;
		}
		return false;
	}

	/**
	 * Compute average of a set of points. Used to find the center of a polygon.
	 * 
	 * @param points
	 *            List of points
	 * @return Center point
	 */
	public static Point average(ArrayList<Point> points) {
		Point avg = new Point(0.0d, 0.0d);
		for (Point point : points) {
			avg = add(avg, point);
		}

		return divide(avg, (double) points.size());
	}

	public static Point add(Point v1, Point v2) {
		return new Point(v1.getX() + v2.getX(), v1.getY() + v2.getY());
	}

	public static Point divide(Point v1, double value) {
		return new Point(v1.getX() / value, v1.getY() / value);
	}

	public Point(double x, double y) {
		this.x = x;
		this.y = y;
	}
	
	public Point(double x, double y, long id){
		this.x = x;
		this.y = y;
		this.id = id;
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	@Override
	public String toString() {
		return x + ", " + y;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}
}
