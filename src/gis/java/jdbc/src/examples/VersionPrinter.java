/*
 * VersionPrinter.java
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

import org.postgis.Version;
import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Prints out as much version information as available.
 */
public class VersionPrinter {

    public static String[] GISVERSIONS = {
        "postgis_version",
        "postgis_proj_version",
        "postgis_scripts_installed",
        "postgis_lib_version",
        "postgis_scripts_released",
        "postgis_uses_stats",
        "postgis_geos_version",
        "postgis_scripts_build_date",
        "postgis_lib_build_date",
        "postgis_full_version"};

    public static void main(String[] args) {
        Statement stat = null;
        Driver d;

    	// Print PostGIS version
        printHeading("PostGIS jdbc client code");
        printVersionString("getFullVersion", Version.getFullVersion());

    	// Print PGJDBC Versions
        printHeading("PGJDBC Driver");
        printVersionString("getVersion", Driver.getVersion());
        try {
            d = new Driver();
        } catch (Exception e) {
            System.err.println("Cannot create Driver instance: " + e.getMessage());
            System.exit(1);
            return;
        }	
        printVersionString("getMajorVersion", d.getMajorVersion());
        printVersionString("getMinorVersion", d.getMinorVersion());

    	// Print PostgreSQL server versions
        if (args.length == 3) {
            Connection conn = null;
            try {
                conn = DriverManager.getConnection(args[0], args[1], args[2]);
                stat = conn.createStatement();
            } catch (SQLException e) {
                System.err.println("Connection to database failed, aborting.");
                System.err.println(e.getMessage());
                System.exit(1);
            }
        } else if (args.length != 0) {
            System.err.println("Usage: java examples/VersionPrinter dburl user pass");
            System.exit(1);
            // Signal the compiler that code flow ends here.
            return;
        }

        if (stat == null) {
            System.out.println("No online version available.");
        }

        printHeading("PostgreSQL Server");
        printVersionString("version", stat);

    	// Print PostGIS versions
        printHeading("PostGIS Server");
        for (int i = 0; i < GISVERSIONS.length; i++) {
            printVersionString(GISVERSIONS[i], stat);
        }

    }

    public static boolean makeemptyline = false;

    private static void printHeading(String heading) {
        if (makeemptyline) {
            System.out.println();
        }
        System.out.println("** " + heading + " **");
        makeemptyline = true;
    }

    public static void printVersionString(String function, int value) {
        printVersionString(function, Integer.toString(value));
    }

    public static void printVersionString(String function, String value) {
        System.out.println("\t" + function + ": " + value);
    }

    public static void printVersionString(String function, Statement stat) {
        printVersionString(function, getVersionString(function, stat));
    }

    public static String getVersionString(String function, Statement stat) {
        try {
            ResultSet rs = stat.executeQuery("SELECT " + function + "()");
            if (rs.next()==false) {
                return "-- no result --";
            }
            String version = rs.getString(1);
            if (version==null) {
                return "-- null result --";
            }
            return version.trim();
        } catch (SQLException e) {
            return "-- unavailable -- ";
        }
    }
}
