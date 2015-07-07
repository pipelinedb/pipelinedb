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

import org.postgis.PGbox2d;
import org.postgis.PGbox3d;
import org.postgresql.util.PGobject;
import org.postgresql.util.PGtokenizer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestBoxes {

    /** Our test candidates: */
    public static final String[] BOXEN3D = new String[]{
        "BOX3D(1 2 3,4 5 6)", // 3d variant
        "BOX3D(1 2,4 5)"// 2d variant
    };
    public static final String[] BOXEN2D = new String[]{"BOX(1 2,3 4)"};

    /** The srid we use for the srid tests */
    public static final int SRID = 4326;

    /** The string prefix we get for the srid tests */
    public static final String SRIDPREFIX = "SRID=" + SRID + ";";

    /** How much tests did fail? */
    public static int failcount = 0;

    /**
     * The actual test method
     */
    public static void test(String orig, PGobject candidate, Connection[] conns,
            boolean newPostgisOnly) throws SQLException {

        System.out.println("Original:  " + orig);
        String redone = candidate.toString();
        System.out.println("Parsed:    " + redone);

        if (!orig.equals(redone)) {
            System.out.println("--- Recreated Text Rep not equal!");
            failcount++;
        }

        // Let's simulate the way pgjdbc uses to create PGobjects
        PGobject recreated;
        try {
            recreated = (PGobject) candidate.getClass().newInstance();
        } catch (Exception e) {
            System.out.println("--- pgjdbc instantiation failed!");
            System.out.println("--- " + e.getMessage());
            failcount++;
            return;
        }
        recreated.setValue(redone);

        String reparsed = recreated.toString();
        System.out.println("Re-Parsed: " + reparsed);
        if (!recreated.equals(candidate)) {
            System.out.println("--- Recreated boxen are not equal!");
            failcount++;
        } else if (!reparsed.equals(orig)) {
            System.out.println("--- 2nd generation text reps are not equal!");
            failcount++;
        } else {
            System.out.println("Equals:    yes");
        }

        for (int i = 0; i < conns.length; i++) {
            System.out.println("Testing on connection " + i + ": " + conns[i].getCatalog());
            Statement statement = conns[i].createStatement();
            if (newPostgisOnly && TestAutoregister.getPostgisMajor(statement) < 1) {
                System.out.println("PostGIS version is too old, not testing box2d");
            } else {

                try {
                    PGobject sqlGeom = viaSQL(candidate, statement);
                    System.out.println("SQLin    : " + sqlGeom.toString());
                    if (!candidate.equals(sqlGeom)) {
                        System.out.println("--- Geometries after SQL are not equal!");
                        failcount++;
                    } else {
                        System.out.println("Eq SQL in: yes");
                    }
                } catch (SQLException e) {
                    System.out.println("--- Server side error: " + e.toString());
                    failcount++;
                }

                try {
                    PGobject sqlreGeom = viaSQL(recreated, statement);
                    System.out.println("SQLout  :  " + sqlreGeom.toString());
                    if (!candidate.equals(sqlreGeom)) {
                        System.out.println("--- reparsed Geometries after SQL are not equal!");
                        failcount++;
                    } else {
                        System.out.println("Eq SQLout: yes");
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

    /** Pass a geometry representation through the SQL server */
    private static PGobject viaSQL(PGobject obj, Statement stat) throws SQLException {
        ResultSet rs = stat.executeQuery("SELECT '" + obj.toString() + "'::" + obj.getType());
        rs.next();
        return (PGobject) rs.getObject(1);
    }

    /**
     * Connect to the databases
     * 
     * We use DriverWrapper here. For alternatives, see the DriverWrapper
     * Javadoc
     * 
     * @param dbuser
     * 
     * @throws ClassNotFoundException
     * 
     * @see org.postgis.DriverWrapper
     * 
     */
    public static Connection connect(String url, String dbuser, String dbpass) throws SQLException,
            ClassNotFoundException {
        Connection conn;
        Class.forName("org.postgis.DriverWrapper");
        conn = DriverManager.getConnection(url, dbuser, dbpass);
        return conn;
    }

    /** Our apps entry point */
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
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

        for (int i = 0; i < BOXEN3D.length; i++) {
            try {
                PGbox3d candidate = new PGbox3d(BOXEN3D[i]);
                test(BOXEN3D[i], candidate, conns, false);
            } catch (SQLException e) {
                System.out.println("--- Instantiation of " + BOXEN3D[i] + "failed:");
                System.out.println("--- " + e.getMessage());
                failcount++;
            }
        }

        for (int i = 0; i < BOXEN2D.length; i++) {
            try {
                PGbox2d candidate = new PGbox2d(BOXEN2D[i]);
                test(BOXEN2D[i], candidate, conns, true);
            } catch (SQLException e) {
                System.out.println("--- Instantiation of " + BOXEN2D[i] + "failed:");
                System.out.println("--- " + e.getMessage());
                failcount++;
            }

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
