/*
 * DriverWrapperAutoprobe.java
 * 
 * PostGIS extension for PostgreSQL JDBC driver - Wrapper utility class
 * 
 * (C) 2005 Markus Schaber, markus.schaber@logix-tt.com
 * 
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 2.1 of the License.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA or visit the web at
 * http://www.gnu.org.
 * 
 */

package org.postgis;

import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;

/**
 * DriverWrapperAutoprobe
 * 
 * Wraps the PostGreSQL Driver to transparently add the PostGIS Object Classes.
 * This avoids the need of explicit addDataType() calls from the driver users
 * side.
 * 
 * This DriverWrapper tries to autoprobe the installed PostGIS version to decide
 * whether to use EWKT or hex encoded EWKB as canonical text representation. It
 * uses the first PostGIS installation found in your namespace search path (aka
 * schema search path) on the server side, and this works as long as you do not
 * access incompatible PostGIS versions that reside in other schemas.
 * 
 * For usage notes, see DriverWrapper class, but use "jdbc:postgresql_autogis:"
 * as JDBC url prefix and org.postgis.DriverWrapperAutoprobe as driver class.
 * 
 * @author Markus Schaber <markus.schaber@logix-tt.com>
 * @see DriverWrapper
 */
public class DriverWrapperAutoprobe extends DriverWrapper {

    public static final String POSTGIS_AUTOPROTOCOL = "jdbc:postgresql_autogis:";
    public static final String REVISIONAUTO = "$Revision$";

    /**
     * Default constructor.
     */
    public DriverWrapperAutoprobe() throws SQLException {
        super();
    }

    static {
        try {
            // Try to register ourself to the DriverManager
            java.sql.DriverManager.registerDriver(new DriverWrapperAutoprobe());
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Error registering PostGIS LW Wrapper Driver", e);
        }
    }

    protected String getProtoString() {
        return POSTGIS_AUTOPROTOCOL;
    }

    protected boolean useLW(Connection conn) {
        try {
            return supportsEWKB(conn);
        } catch (SQLException e) {
            // fail safe default
            return false;
        }
    }

    /**
     * Returns our own CVS version plus postgres Version
     */
    public static String getVersion() {
        return "PostGisWrapperAutoprobe " + REVISIONAUTO + ", wrapping " + Driver.getVersion();
    }

    public static boolean supportsEWKB(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT postgis_version()");
        rs.next();
        String version = rs.getString(1);
        rs.close();
        stat.close();
        if (version == null) {
            throw new SQLException("postgis_version returned NULL!");
        }
        version = version.trim();
        int idx = version.indexOf('.');
        int majorVersion = Integer.parseInt(version.substring(0, idx));
        return majorVersion >= 1;
    }
}
