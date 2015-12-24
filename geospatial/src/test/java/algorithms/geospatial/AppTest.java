package algorithms.geospatial;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import math.geom2d.Point2D;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
    
    public void testPolygon()
    {
    	Point2D point = new Point2D(10, 10);
    	assertTrue(point != null);
    }
    
}
