package algorithms.geospatial.math;

import java.io.Serializable;

/**
 * Class to represent a line segment (2 points)
 * 
 * @author pramodh
 *
 */
@SuppressWarnings("serial")
public class LineSegment implements Serializable {

	/**
	 * Ends of a line segment
	 */
	private Point p1, p2;

	public Point getP1() {
		return p1;
	}

	public void setP1(Point p1) {
		this.p1 = p1;
	}

	public Point getP2() {
		return p2;
	}

	public void setP2(Point p2) {
		this.p2 = p2;
	}

	public LineSegment(Point p1, Point p2) {
		this.p1 = p1;
		this.p2 = p2;
	}

	/**
	 * Compute the length of the line segment.
	 * 
	 * @return length of the line segment.
	 */
	public double distance() {
		return Math.sqrt(Math.pow(p1.getX() - p2.getX(), 2) + Math.pow(p1.getY() - p2.getY(), 2));
	}

	@Override
	public String toString() {
		return p1 + "\n" + p2;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		LineSegment l = (LineSegment) obj;
		if (this.distance() != l.distance())
			return false;
		else
			return true;
	}

}
