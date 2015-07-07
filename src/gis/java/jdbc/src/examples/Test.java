/*
 * Test.java
 * 
 * PostGIS extension for PostgreSQL JDBC driver - example and test classes
 * 
 * (C) 2004 Paul Ramsey, pramsey@refractions.net
 * 
 * (C) 2005 Markus Schaber, markus.schaber@logix-tt.com
 * 
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 59 Temple
 * Place, Suite 330, Boston, MA 02111-1307 USA or visit the web at
 * http://www.gnu.org.
 * 
 */

package examples;

import org.postgis.LineString;
import org.postgis.LinearRing;
import org.postgis.MultiLineString;
import org.postgis.MultiPolygon;
import org.postgis.PGgeometry;
import org.postgis.Point;
import org.postgis.Polygon;

import java.sql.SQLException;

public class Test {

    public static void main(String[] args) throws SQLException {
        String mlng_str = "MULTILINESTRING ((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0))";
        String mplg_str = "MULTIPOLYGON (((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)),((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)))";
        String plg_str = "POLYGON ((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0))";
        String lng_str = "LINESTRING  (10 10 20,20 20 20, 50 50 50, 34 34 34)";
        String ptg_str = "POINT(10 10 20)";
        String lr_str = "(10 10 20,34 34 34, 23 19 23 , 10 10 11)";

        System.out.println("LinearRing Test:");
        System.out.println("\t" + lr_str);
        LinearRing lr = new LinearRing(lr_str);
        System.out.println("\t" + lr.toString());

        System.out.println();

        System.out.println("Point Test:");
        System.out.println("\t" + ptg_str);
        Point ptg = new Point(ptg_str);
        System.out.println("\t" + ptg.toString());

        System.out.println();

        System.out.println("LineString Test:");
        System.out.println("\t" + lng_str);
        LineString lng = new LineString(lng_str);
        System.out.println("\t" + lng.toString());

        System.out.println();

        System.out.println("Polygon Test:");
        System.out.println("\t" + plg_str);
        Polygon plg = new Polygon(plg_str);
        System.out.println("\t" + plg.toString());

        System.out.println();

        System.out.println("MultiPolygon Test:");
        System.out.println("\t" + mplg_str);
        MultiPolygon mplg = new MultiPolygon(mplg_str);
        System.out.println("\t" + mplg.toString());

        System.out.println();

        System.out.println("MultiLineString Test:");
        System.out.println("\t" + mlng_str);
        MultiLineString mlng = new MultiLineString(mlng_str);
        System.out.println("\t" + mlng.toString());

        System.out.println();

        System.out.println("PG Test:");
        System.out.println("\t" + mlng_str);
        PGgeometry pgf = new PGgeometry(mlng_str);
        System.out.println("\t" + pgf.toString());

        System.out.println();

        System.out.println("finished");
        // If we reached here without any exception, we passed all tests.
        System.err.println("Test.java finished without errors.");
    }
}
