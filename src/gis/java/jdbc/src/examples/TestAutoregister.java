/*
 * TestAutoregister.java
 * 
 * PostGIS extension for PostgreSQL JDBC driver - example and test classes
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
import org.postgis.PGgeometry;
import org.postgresql.Driver;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This test program tests whether the autoregistration of PostGIS data types
 * within the pgjdbc driver was successful. This is supposed to work with
 * pgjdbc.jar version 8.0 and newer, and thus refuses to work with older pgjdbc
 * versions. (But it will work fine against older servers.) It also checks for
 * postgis version to know whether box2d is available.
 */
public class TestAutoregister {

    public static void main(String[] args) {
        String dburl = null;
        String dbuser = null;
        String dbpass = null;

        if (args.length == 3) {
            System.out.println("Testing proper auto-registration");
            dburl = args[0];
            dbuser = args[1];
            dbpass = args[2];
        } else {
            System.err.println("Usage: java examples/TestParser dburl user pass");
            System.exit(1);
            // Signal the compiler that code flow ends here.
            return;
        }

        System.out.println("Driver version: " + Driver.getVersion());
        int major;
        try {
            major = new Driver().getMajorVersion();
        } catch (Exception e) {
            System.err.println("Cannot create Driver instance: " + e.getMessage());
            System.exit(1);
            return;
        }

        if (major < 8) {
            System.err.println("Your pgdjbc " + major
                    + ".X is too old, it does not support autoregistration!");
            return;
        }

        System.out.println("Creating JDBC connection to " + dburl);
        Connection conn = null;
        Statement stat = null;
        try {
            conn = DriverManager.getConnection(dburl, dbuser, dbpass);
            stat = conn.createStatement();
        } catch (SQLException e) {
            System.err.println("Connection initialization failed, aborting.");
            e.printStackTrace();
            System.exit(1);
            // signal the compiler that code flow ends here:
            throw new AssertionError();
        }

        int postgisServerMajor = 0;
        try {
            postgisServerMajor = getPostgisMajor(stat);
        } catch (SQLException e) {
            System.err.println("Error fetching PostGIS version: " + e.getMessage());
            System.err.println("Is PostGIS really installed in the database?");
             System.exit(1);
            // signal the compiler that code flow ends here:
            throw new AssertionError();
        }

        System.out.println("PostGIS Version: " + postgisServerMajor);

        PGobject result = null;

        /* Test geometries */
        try {
            ResultSet rs = stat.executeQuery("SELECT 'POINT(1 2)'::geometry");
            rs.next();
            result = (PGobject) rs.getObject(1);
            if (result instanceof PGgeometry) {
                System.out.println("PGgeometry successful!");
            } else {
                System.out.println("PGgeometry failed!");
            }
        } catch (SQLException e) {
            System.err.println("Selecting geometry failed: " + e.getMessage());
            System.exit(1);
            // Signal the compiler that code flow ends here.
            return;
        }

        /* Test box3d */
        try {
            ResultSet rs = stat.executeQuery("SELECT 'BOX3D(1 2 3, 4 5 6)'::box3d");
            rs.next();
            result = (PGobject) rs.getObject(1);
            if (result instanceof PGbox3d) {
                System.out.println("Box3d successful!");
            } else {
                System.out.println("Box3d failed!");
            }
        } catch (SQLException e) {
            System.err.println("Selecting box3d failed: " + e.getMessage());
            System.exit(1);
            // Signal the compiler that code flow ends here.
            return;
        }

        /* Test box2d if appropriate */
        if (postgisServerMajor < 1) {
            System.out.println("PostGIS version is too old, skipping box2ed test");
            System.err.println("PostGIS version is too old, skipping box2ed test");
        } else {
            try {
                ResultSet rs = stat.executeQuery("SELECT 'BOX(1 2,3 4)'::box2d");
                rs.next();
                result = (PGobject) rs.getObject(1);
                if (result instanceof PGbox2d) {
                    System.out.println("Box2d successful!");
                } else {
                    System.out.println("Box2d failed! " + result.getClass().getName());
                }
            } catch (SQLException e) {
                System.err.println("Selecting box2d failed: " + e.getMessage());
                System.exit(1);
                // Signal the compiler that code flow ends here.
                return;
            }
        }

        System.out.println("Finished.");
        // If we finished up to here without exitting, we passed all tests.
        System.err.println("TestAutoregister.java finished without errors.");
    }

    public static int getPostgisMajor(Statement stat) throws SQLException {
        ResultSet rs = stat.executeQuery("SELECT postgis_version()");
        rs.next();
        String version = rs.getString(1);
        if (version == null) {
            throw new SQLException("postgis_version returned NULL!");
        }
        version = version.trim();
        int idx = version.indexOf('.');
        return Integer.parseInt(version.substring(0, idx));
    }
}
