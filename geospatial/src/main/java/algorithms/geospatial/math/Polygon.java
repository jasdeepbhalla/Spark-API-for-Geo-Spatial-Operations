package algorithms.geospatial.math;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import math.geom2d.Point2D;
import math.geom2d.polygon.Polygon2D;
import math.geom2d.polygon.Polygons2D;
import math.geom2d.polygon.SimplePolygon2D;
import math.geom2d.polygon.convhull.JarvisMarch2D;

/**
 * Class to maintain polygon structure
 * 
 * @author Pramodh
 *
 */
@SuppressWarnings("serial")
public class Polygon implements Serializable {

	/**
	 * Vertices of the polygon
	 */
	private ArrayList<Point> points;

	private long id;

	/**
	 * Create polygon from a list of vertices
	 * 
	 * @param points
	 */
	public Polygon(ArrayList<Point> points) {
		this.points = points;
	}

	/**
	 * Create polygon from given end points of a rectangle.
	 * 
	 * @param btmLeft
	 * @param topRight
	 */
	public Polygon(Point btmLeft, Point topRight, long id) {
		this.setId(id);
		Point topLeft = new Point(btmLeft.getX(), topRight.getY());
		Point btmRight = new Point(topRight.getX(), btmLeft.getY());
		this.points = new ArrayList<Point>();
		this.points.add(btmLeft);
		this.points.add(topLeft);
		this.points.add(topRight);
		this.points.add(btmRight);
	}

	/**
	 * Create polygon containing just one point
	 */
	public Polygon(double x1, double x2) {
		this.points = new ArrayList<Point>();
		this.points.add(new Point(x1, x2));
	}

	/**
	 * Compute center of polygon
	 * 
	 * @return The center point of the polygon
	 */
	public Point center() {
		return Point.average(points);
	}

	/**
	 * Compute area of this polygon. Ref - http://www.mathopenref.com/coordpolygonarea2.html
	 * 
	 * @return Area of the polygon.
	 */
	public double area() {
		double area = 0;
		for (int i = 0; i < points.size() - 1; ++i) {
			area += points.get(i).getX() * points.get(i + 1).getY();
			area -= points.get(i).getY() * points.get(i + 1).getX();
		}
		return Math.abs(area * 0.5d);
	}

	/**
	 * Check if this contains a given point.
	 * 
	 * @param pt
	 *            The point that is to be checked.
	 * @return True if polygon contains point. False if not.
	 */
	public boolean contains(Point pt) {
		int i;
		boolean c = false;
		int n = points.size();

		// jordan curve theorem
		for (i = 0; i < n; i++) {
			if (((points.get(i).getY() >= pt.getY()) != (points.get((i + 1) % n).getY() >= pt.getY())) && (pt
					.getX() <= (points.get((i + 1) % n).getX() - points.get(i).getX()) * (pt.getY() - points.get(i).getY()) / (points.get((i + 1) % n).getY() - points.get(i).getY())
							+ points.get(i).getX())) {
				c = !c;
			}
		}
		return c;
	}

	/**
	 * Checks if a given polygon is inside this polygon
	 * 
	 * @param polygon
	 *            The given polygon
	 * @return True if this polygon contains given polygon. False if not.
	 */
	public Boolean isOverlap(Polygon polygon) {
		ArrayList<Point2D> polygonPoints = new ArrayList<Point2D>();
		for (Point point : polygon.getPoints()) {
			polygonPoints.add(new Point2D(point.getX(), point.getY()));
		}
		SimplePolygon2D polygon1 = new SimplePolygon2D(polygonPoints);

		ArrayList<Point2D> thisPolygonPoints = new ArrayList<Point2D>();
		for (Point point : this.getPoints()) {
			thisPolygonPoints.add(new Point2D(point.getX(), point.getY()));
		}
		SimplePolygon2D polygon2 = new SimplePolygon2D(thisPolygonPoints);
		
		Polygon2D intersection = Polygons2D.intersection(polygon1, polygon2);
		
		if(intersection.vertices().size() == 0)
			return false;
		else
			return true;
	}

	/**
	 * Compute union of this polygon with a given polygon.
	 * 
	 * @param polygon
	 *            The polygon with which union is to be calculated.
	 * @return The union polygon
	 */
	public Polygon union(Polygon polygon) {
		ArrayList<Point2D> polygonPoints = new ArrayList<Point2D>();
		for (Point point : polygon.getPoints()) {
			polygonPoints.add(new Point2D(point.getX(), point.getY()));
		}
		SimplePolygon2D polygon1 = new SimplePolygon2D(polygonPoints);

		ArrayList<Point2D> thisPolygonPoints = new ArrayList<Point2D>();
		for (Point point : this.getPoints()) {
			thisPolygonPoints.add(new Point2D(point.getX(), point.getY()));
		}
		SimplePolygon2D polygon2 = new SimplePolygon2D(thisPolygonPoints);

		SimplePolygon2D union = (SimplePolygon2D) Polygons2D.union(polygon1, polygon2);

		ArrayList<Point> unionPoints = new ArrayList<Point>();
		for (Point2D vertex : union.vertices()) {
			unionPoints.add(new Point(vertex.x(), vertex.y()));
		}

		return new Polygon(unionPoints);
	}

	/**
	 * Compute convex hull from a given polygon and this polygon.
	 * 
	 * @param polygon
	 *            The given polygon
	 * @return The convex hull
	 */
	public Polygon convexHull(Polygon polygon) {
		Set<Point2D> points = new HashSet<Point2D>();
		for (Point p : polygon.getPoints()) {
			points.add(new Point2D(p.getX(), p.getY()));
		}
		for (Point p : this.getPoints()) {
			points.add(new Point2D(p.getX(), p.getY()));
		}

		JarvisMarch2D convexHull = new JarvisMarch2D();
		Polygon2D convexPolygon = convexHull.convexHull(points);

		ArrayList<Point> convexPolygonVertices = new ArrayList<Point>();
		for (Point2D p : convexPolygon.vertices()) {
			convexPolygonVertices.add(new Point(p.x(), p.y()));
		}

		return new Polygon(convexPolygonVertices);
	}

	public ArrayList<Point> getPoints() {
		return points;
	}

	public void setPoints(ArrayList<Point> points) {
		this.points = points;
	}

	@Override
	public String toString() {
		return points.toString();
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}
}
