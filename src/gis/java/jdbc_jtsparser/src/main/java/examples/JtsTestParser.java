/*
 * JtsTestParser.java
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.postgis.binary.ValueSetter;
import org.postgis.jts.JtsBinaryParser;
import org.postgis.jts.JtsBinaryWriter;
import org.postgis.jts.JtsGeometry;
import org.postgresql.util.PGtokenizer;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class JtsTestParser {

    public static String ALL = "ALL", ONLY10 = "ONLY10", EQUAL10 = "EQUAL10";

    /**
     * Our set of geometries to test.
     */
    public static final String[][] testset = new String[][] {
            { ALL, // 2D
                    "POINT(10 10)" },
            { ALL, // 3D with 3rd coordinate set to 0
                    "POINT(10 10 0)" },
            { ALL, // 3D
                    "POINT(10 10 20)" },
            { ALL, "MULTIPOINT(11 12, 20 20)" },
            { ALL, "MULTIPOINT(11 12 13, 20 20 20)" },
            { ALL, "LINESTRING(10 10,20 20,50 50,34 34)" },
            { ALL, "LINESTRING(10 10 20,20 20 20,50 50 50,34 34 34)" },
            { ALL, "POLYGON((10 10,20 10,20 20,20 10,10 10),(5 5,5 6,6 6,6 5,5 5))" },
            { ALL, "POLYGON((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0))" },
            {
                    ALL,
                    "MULTIPOLYGON(((10 10,20 10,20 20,20 10,10 10),(5 5,5 6,6 6,6 5,5 5)),((10 10,20 10,20 20,20 10,10 10),(5 5,5 6,6 6,6 5,5 5)))" },
            {
                    ALL,
                    "MULTIPOLYGON(((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)),((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)))" },
            { ALL, "MULTILINESTRING((10 10,20 10,20 20,20 10,10 10),(5 5,5 6,6 6,6 5,5 5))" },
            { ALL, "MULTILINESTRING((10 10 5,20 10 5,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0))" },
            { ALL, "GEOMETRYCOLLECTION(POINT(10 10),POINT(20 20))" },
            { ALL, "GEOMETRYCOLLECTION(POINT(10 10 20),POINT(20 20 20))" },
            {
                    ALL,
                    "GEOMETRYCOLLECTION(LINESTRING(10 10 20,20 20 20, 50 50 50, 34 34 34),LINESTRING(10 10 20,20 20 20, 50 50 50, 34 34 34))" },
            {
                    ALL,
                    "GEOMETRYCOLLECTION(POLYGON((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)),POLYGON((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)))" },
            { ONLY10, // Cannot be parsed by 0.X servers
                    "GEOMETRYCOLLECTION(MULTIPOINT(10 10 10, 20 20 20),MULTIPOINT(10 10 10, 20 20 20))" },
            { EQUAL10, // PostGIs 0.X "flattens" this geometry, so it is not
                    // equal after reparsing.
                    "GEOMETRYCOLLECTION(MULTILINESTRING((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)))" },
            { EQUAL10,// PostGIs 0.X "flattens" this geometry, so it is not
                        // equal
                    // after reparsing.
                    "GEOMETRYCOLLECTION(MULTIPOLYGON(((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)),((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0))),MULTIPOLYGON(((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)),((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0))))" },
            {
                    ALL,
                    "GEOMETRYCOLLECTION(POINT(10 10 20),LINESTRING(10 10 20,20 20 20, 50 50 50, 34 34 34),POLYGON((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)))" },
            { ONLY10, // Collections that contain both X and MultiX do not
                        // work on
                    // PostGIS 0.x
                    "GEOMETRYCOLLECTION(POINT(10 10 20),MULTIPOINT(10 10 10, 20 20 20),LINESTRING(10 10 20,20 20 20, 50 50 50, 34 34 34),POLYGON((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)),MULTIPOLYGON(((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)),((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0))),MULTILINESTRING((10 10 0,20 10 0,20 20 0,20 10 0,10 10 0),(5 5 0,5 6 0,6 6 0,6 5 0,5 5 0)))" },
            { ALL,// new (correct) representation
                    "GEOMETRYCOLLECTION EMPTY" },
    // end
    };

    /** The srid we use for the srid tests */
    public static final int SRID = 4326;

    /** The string prefix we get for the srid tests */
    public static final String SRIDPREFIX = "SRID=" + SRID + ";";

    /** How much tests did fail? */
    public static int failcount = 0;

    private static JtsBinaryParser bp = new JtsBinaryParser();

    private static final JtsBinaryWriter bw = new JtsBinaryWriter();

    /** The actual test method */
    public static void test(String WKT, Connection[] conns, String flags) throws SQLException {
        System.out.println("Original:  " + WKT);
        Geometry geom = JtsGeometry.geomFromString(WKT);
        String parsed = geom.toString();
        if (WKT.startsWith("SRID=")) {
            parsed = "SRID="+geom.getSRID()+";"+parsed;
        }
        System.out.println("Parsed:    " + parsed);
        Geometry regeom = JtsGeometry.geomFromString(parsed);
        String reparsed = regeom.toString();
        if (WKT.startsWith("SRID=")) {
            reparsed = "SRID="+geom.getSRID()+";"+reparsed;
        }
        System.out.println("Re-Parsed: " + reparsed);
        if (!geom.equalsExact(regeom)) {
            System.out.println("--- Geometries are not equal!");
            failcount++;
        } else if (geom.getSRID() != regeom.getSRID()) {
            System.out.println("--- Geometriy SRIDs are not equal!");
            failcount++;
        } else if (!reparsed.equals(parsed)) {
            System.out.println("--- Text Reps are not equal!");
            failcount++;
        } else {
            System.out.println("Equals:    yes");
        }

        String hexNWKT = bw.writeHexed(geom, ValueSetter.NDR.NUMBER);
        System.out.println("NDRHex:    " + hexNWKT);
        regeom = JtsGeometry.geomFromString(hexNWKT);
        System.out.println("ReNDRHex:  " + regeom.toString());
        if (!geom.equalsExact(regeom)) {
            System.out.println("--- Geometries are not equal!");
            failcount++;
        } else {
            System.out.println("Equals:    yes");
        }

        String hexXWKT = bw.writeHexed(geom, ValueSetter.XDR.NUMBER);
        System.out.println("XDRHex:    " + hexXWKT);
        regeom = JtsGeometry.geomFromString(hexXWKT);
        System.out.println("ReXDRHex:  " + regeom.toString());
        if (!geom.equalsExact(regeom)) {
            System.out.println("--- Geometries are not equal!");
            failcount++;
        } else {
            System.out.println("Equals:    yes");
        }

        byte[] NWKT = bw.writeBinary(geom, ValueSetter.NDR.NUMBER);
        regeom = bp.parse(NWKT);
        System.out.println("NDR:       " + regeom.toString());
        if (!geom.equalsExact(regeom)) {
            System.out.println("--- Geometries are not equal!");
            failcount++;
        } else {
            System.out.println("Equals:    yes");
        }

        byte[] XWKT = bw.writeBinary(geom, ValueSetter.XDR.NUMBER);
        regeom = bp.parse(XWKT);
        System.out.println("XDR:       " + regeom.toString());
        if (!geom.equalsExact(regeom)) {
            System.out.println("--- Geometries are not equal!");
            failcount++;
        } else {
            System.out.println("Equals:    yes");
        }

        Geometry coordArrayGeom = rebuildCS(geom);
        System.out.println("CoordArray:" + regeom.toString());
        if (!geom.equalsExact(coordArrayGeom)) {
            System.out.println("--- Geometries are not equal!");
            failcount++;
        } else {
            System.out.println("Equals:    yes");
        }

        String coordArrayWKT = bw.writeHexed(coordArrayGeom, ValueSetter.NDR.NUMBER);
        System.out.println("HexCArray: " + coordArrayWKT);
        if (!coordArrayWKT.equals(hexNWKT)) {
            System.out.println("--- CoordArray HexWKT is not equal: "+bp.parse(coordArrayWKT));
            failcount++;
        } else {
            System.out.println("HexEquals: yes");
        }

        for (int i = 0; i < conns.length; i++) {
            Connection connection = conns[i];
            Statement statement = connection.createStatement();
            int serverPostgisMajor = TestAutoregister.getPostgisMajor(statement);

            if ((flags == ONLY10) && serverPostgisMajor < 1) {
                System.out.println("PostGIS server too old, skipping test on connection " + i + ": "
                        + connection.getCatalog());
            } else {
                System.out.println("Testing on connection " + i + ": " + connection.getCatalog());
                try {
                    Geometry sqlGeom = viaSQL(WKT, statement);
                    System.out.println("SQLin    : " + sqlGeom.toString());
                    if (!geom.equalsExact(sqlGeom)) {
                        System.out.println("--- Geometries after SQL are not equal!");
                        if (flags == EQUAL10 && serverPostgisMajor < 1) {
                            System.out.println("--- This is expected with PostGIS " + serverPostgisMajor + ".X");
                        } else {
                            failcount++;
                        }
                    } else {
                        System.out.println("Eq SQL in: yes");
                    }
                } catch (SQLException e) {
                    System.out.println("--- Server side error: " + e.toString());
                    failcount++;
                }

                try {
                    Geometry sqlreGeom = viaSQL(parsed, statement);
                    System.out.println("SQLout  :  " + sqlreGeom.toString());
                    if (!geom.equalsExact(sqlreGeom)) {
                        System.out.println("--- reparsed Geometries after SQL are not equal!");
                        if (flags == EQUAL10 && serverPostgisMajor < 1) {
                            System.out.println("--- This is expected with PostGIS " + serverPostgisMajor + ".X");
                        } else {
                            failcount++;
                        }
                    } else {
                        System.out.println("Eq SQLout: yes");
                    }
                } catch (SQLException e) {
                    System.out.println("--- Server side error: " + e.toString());
                    failcount++;
                }

                try {
                    Geometry sqlreGeom = viaPrepSQL(geom, connection);
                    System.out.println("Prepared:  " + sqlreGeom.toString());
                    if (!geom.equalsExact(sqlreGeom)) {
                        System.out.println("--- reparsed Geometries after prepared StatementSQL are not equal!");
                        if (flags == EQUAL10 && serverPostgisMajor < 1) {
                            System.out.println("--- This is expected with PostGIS " + serverPostgisMajor + ".X");
                        } else {
                            failcount++;
                        }
                    } else {
                        System.out.println("Eq Prep: yes");
                    }
                } catch (SQLException e) {
                    System.out.println("--- Server side error: " + e.toString());
                    failcount++;
                }

                // asEWKT() function is not present on PostGIS 0.X, and the test
                // is pointless as 0.X uses EWKT as canonical rep so the same
                // functionality was already tested above.
                try {
                    if (serverPostgisMajor >= 1) {
                        Geometry sqlGeom = ewktViaSQL(WKT, statement);
                        System.out.println("asEWKT   : " + sqlGeom.toString());
                        if (!geom.equalsExact(sqlGeom)) {
                            System.out.println("--- Geometries after EWKT SQL are not equal!");
                            failcount++;
                        } else {
                            System.out.println("equal   : yes");
                        }
                    }
                } catch (SQLException e) {
                    System.out.println("--- Server side error: " + e.toString());
                    failcount++;
                }

                // asEWKB() function is not present on PostGIS 0.X.
                try {
                    if (serverPostgisMajor >= 1) {
                        Geometry sqlGeom = ewkbViaSQL(WKT, statement);
                        System.out.println("asEWKB   : " + sqlGeom.toString());
                        if (!geom.equalsExact(sqlGeom)) {
                            System.out.println("--- Geometries after EWKB SQL are not equal!");
                            failcount++;
                        } else {
                            System.out.println("equal    : yes");
                        }
                    }
                } catch (SQLException e) {
                    System.out.println("--- Server side error: " + e.toString());
                    failcount++;
                }

                // HexEWKB parsing is not present on PostGIS 0.X.
                try {
                    if (serverPostgisMajor >= 1) {
                        Geometry sqlGeom = viaSQL(hexNWKT, statement);
                        System.out.println("hexNWKT:   " + sqlGeom.toString());
                        if (!geom.equalsExact(sqlGeom)) {
                            System.out.println("--- Geometries after EWKB SQL are not equal!");
                            failcount++;
                        } else {
                            System.out.println("equal    : yes");
                        }
                    }
                } catch (SQLException e) {
                    System.out.println("--- Server side error: " + e.toString());
                    failcount++;
                }
                try {
                    if (serverPostgisMajor >= 1) {
                        Geometry sqlGeom = viaSQL(hexXWKT, statement);
                        System.out.println("hexXWKT:   " + sqlGeom.toString());
                        if (!geom.equalsExact(sqlGeom)) {
                            System.out.println("--- Geometries after EWKB SQL are not equal!");
                            failcount++;
                        } else {
                            System.out.println("equal    : yes");
                        }
                    }
                } catch (SQLException e) {
                    System.out.println("--- Server side error: " + e.toString());
                    failcount++;
                }

                // Canonical binary input is not present before 1.0
                try {
                    if (serverPostgisMajor >= 1) {
                        Geometry sqlGeom = binaryViaSQL(NWKT, connection);
                        System.out.println("NWKT:      " + sqlGeom.toString());
                        if (!geom.equalsExact(sqlGeom)) {
                            System.out.println("--- Geometries after EWKB SQL are not equal!");
                            failcount++;
                        } else {
                            System.out.println("equal    : yes");
                        }
                    }
                } catch (SQLException e) {
                    System.out.println("--- Server side error: " + e.toString());
                    failcount++;
                }
                try {
                    if (serverPostgisMajor >= 1) {
                        Geometry sqlGeom = binaryViaSQL(XWKT, connection);
                        System.out.println("XWKT:      " + sqlGeom.toString());
                        if (!geom.equalsExact(sqlGeom)) {
                            System.out.println("--- Geometries after EWKB SQL are not equal!");
                            failcount++;
                        } else {
                            System.out.println("equal    : yes");
                        }
                    }
                } catch (SQLException e) {
                    System.out.println("--- Server side error: " + e.toString());
                    failcount++;
                }

            }
            statement.close();
        }
        System.out.println("***");
    }

    // Rebuild given Geometry with a CoordinateArraySequence implementation. 
    public static Geometry rebuildCS(Geometry geom) {        
        if (geom instanceof Point) {
            return rebuildCSPoint((Point)geom);
        } else if (geom instanceof MultiPoint) {
            return rebuildCSMP((MultiPoint)geom);
        } else if (geom instanceof LineString) {
            return rebuildCSLS((LineString)geom);
        } else if (geom instanceof MultiLineString) {
            return rebuildCSMLS((MultiLineString)geom);
        } else if (geom instanceof Polygon) {
            return rebuildCSP((Polygon)geom);
        } else if (geom instanceof MultiPolygon) {
            return rebuildCSMP((MultiPolygon)geom);
        } else if (geom instanceof GeometryCollection) {
            return rebuildCSGC((GeometryCollection)geom);
        } else {
            throw new AssertionError();
        }
    }

    private static Geometry rebuildCSGC(GeometryCollection coll) {
        Geometry[] geoms = new Geometry[coll.getNumGeometries()];
        for (int i = 0; i < coll.getNumGeometries(); i++) {
            geoms[i] = rebuildCS(coll.getGeometryN(i));
        }
        Geometry result = coll.getFactory().createGeometryCollection(geoms);
        result.setSRID(coll.getSRID());
        return result;
    }

    private static MultiPolygon rebuildCSMP(MultiPolygon multipoly) {
        Polygon[] polygons = new Polygon[multipoly.getNumGeometries()];
        for (int i=0; i < polygons.length; i++) {
            polygons[i] = rebuildCSP((Polygon)multipoly.getGeometryN(i));
        }
        MultiPolygon result = multipoly.getFactory().createMultiPolygon(polygons);
        result.setSRID(multipoly.getSRID());
        return result;
    }

    private static Polygon rebuildCSP(Polygon polygon) {
        LinearRing outer = rebuildLR(polygon.getExteriorRing());
        LinearRing[] holes = new LinearRing[polygon.getNumInteriorRing()];
        for (int i=0; i < holes.length; i++) {
            holes[i] = rebuildLR(polygon.getInteriorRingN(i));
        }
        Polygon result = polygon.getFactory().createPolygon(outer, holes);
        result.setSRID(polygon.getSRID());
        return result;
    }

    private static LinearRing rebuildLR(LineString ring) {
        LinearRing result = ring.getFactory().createLinearRing(ring.getCoordinates());
        result.setSRID(ring.getSRID());
        return result;
    }

    private static MultiLineString rebuildCSMLS(MultiLineString multiline) {
        LineString[] polygons = new LineString[multiline.getNumGeometries()];
        for (int i=0; i < polygons.length; i++) {
            polygons[i] = rebuildCSLS((LineString)multiline.getGeometryN(i));
        }
        MultiLineString result = multiline.getFactory().createMultiLineString(polygons);
        result.setSRID(multiline.getSRID());
        return result;
        
    }

    private static LineString rebuildCSLS(LineString line) {
        LineString result = line.getFactory().createLineString(line.getCoordinates());
        result.setSRID(line.getSRID());
        return result; 
    }

    private static MultiPoint rebuildCSMP(MultiPoint mp) {
        Point[] points = new Point[mp.getNumGeometries()];
        for (int i=0; i < points.length; i++) {
            points[i] = rebuildCSPoint((Point) mp.getGeometryN(i));
        }
        MultiPoint result = mp.getFactory().createMultiPoint(points);
        result.setSRID(mp.getSRID());
        return result;
    }

    private static Point rebuildCSPoint(Point point) {
        Point result = point.getFactory().createPoint(point.getCoordinate());
        result.setSRID(point.getSRID());
        return result;       
    }

    /** Pass a geometry representation through the SQL server */
    private static Geometry viaSQL(String rep, Statement stat) throws SQLException {
        ResultSet rs = stat.executeQuery("SELECT geometry_in('" + rep + "')");
        rs.next();
        return ((JtsGeometry) rs.getObject(1)).getGeometry();
    }

    /**
     * Pass a geometry representation through the SQL server via prepared
     * statement
     */
    private static Geometry viaPrepSQL(Geometry geom, Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?::geometry");
        JtsGeometry wrapper = new JtsGeometry(geom);
        prep.setObject(1, wrapper, Types.OTHER);
        ResultSet rs = prep.executeQuery();
        rs.next();
        JtsGeometry resultwrapper = ((JtsGeometry) rs.getObject(1));
        return resultwrapper.getGeometry();
    }

    /** Pass a geometry representation through the SQL server via EWKT */
    private static Geometry ewktViaSQL(String rep, Statement stat) throws SQLException {
        ResultSet rs = stat.executeQuery("SELECT asEWKT(geometry_in('" + rep + "'))");
        rs.next();
        String resrep = rs.getString(1);
        return JtsGeometry.geomFromString(resrep);
    }

    /** Pass a geometry representation through the SQL server via EWKB */
    private static Geometry ewkbViaSQL(String rep, Statement stat) throws SQLException {
        ResultSet rs = stat.executeQuery("SELECT asEWKB(geometry_in('" + rep + "'))");
        rs.next();
        byte[] resrep = rs.getBytes(1);
        return bp.parse(resrep);
    }

    /** Pass a EWKB geometry representation through the server */
    private static Geometry binaryViaSQL(byte[] rep, Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?::bytea::geometry");
        prep.setBytes(1, rep);
        ResultSet rs = prep.executeQuery();
        rs.next();
        JtsGeometry resultwrapper = ((JtsGeometry) rs.getObject(1));
        return resultwrapper.getGeometry();
    }

    /**
     * Connect to the databases
     * 
     * We use DriverWrapper here. For alternatives, see the DriverWrapper
     * Javadoc
     * 
     * @param dbuser
     * 
     * @see org.postgis.DriverWrapper
     * 
     */
    public static Connection connect(String url, String dbuser, String dbpass) throws SQLException {
        Connection conn;
        conn = DriverManager.getConnection(url, dbuser, dbpass);
        return conn;
    }

    public static void loadDrivers() throws ClassNotFoundException {
        Class.forName("org.postgis.jts.JtsWrapper");
    }

    /** Our apps entry point */
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        loadDrivers();

        PGtokenizer dburls;
        String dbuser = null;
        String dbpass = null;

        if (args.length == 1 && args[0].equalsIgnoreCase("offline")) {
            System.out.println("Performing only offline tests");
            dburls = new PGtokenizer("", ';');
        } else if (args.length == 3) {
            System.out.println("Performing offline and online tests");
            dburls = new PGtokenizer(args[0], ';');

            dbuser = args[1];
            dbpass = args[2];
        } else {
            System.err.println("Usage: java examples/TestParser dburls user pass [tablename]");
            System.err.println("   or: java examples/TestParser offline");
            System.err.println();
            System.err.println("dburls has one or more jdbc urls separated by ; in the following format");
            System.err.println("jdbc:postgresql://HOST:PORT/DATABASENAME");
            System.err.println("tablename is 'jdbc_test' by default.");
            System.exit(1);
            // Signal the compiler that code flow ends here.
            return;
        }

        Connection[] conns;
        conns = new Connection[dburls.getSize()];
        for (int i = 0; i < dburls.getSize(); i++) {
            System.out.println("Creating JDBC connection to " + dburls.getToken(i));
            conns[i] = connect(dburls.getToken(i), dbuser, dbpass);
        }

        System.out.println("Performing tests...");
        System.out.println("***");

        for (int i = 0; i < testset.length; i++) {
            test(testset[i][1], conns, testset[i][0]);
            test(SRIDPREFIX + testset[i][1], conns, testset[i][0]);
        }

        System.out.print("cleaning up...");
        for (int i = 0; i < conns.length; i++) {
            conns[i].close();
        }

        System.out.println("Finished, " + failcount + " tests failed!");
        System.err.println("Finished, " + failcount + " tests failed!");
        System.exit(failcount);
    }
}
