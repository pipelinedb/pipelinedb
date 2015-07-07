/*
 * DriverWrapperLW.java
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
import java.sql.SQLException;
import java.util.logging.Level;

/**
 * DriverWrapperLW
 * 
 * Wraps the PostGreSQL Driver to transparently add the PostGIS Object Classes.
 * This avoids the need of explicit addDataType() calls from the driver users
 * side.
 * 
 * This DriverWrapper subclass always uses hex encoded EWKB as canonical text
 * representation, and thus only works against PostGIS 1.x servers and newer.
 * 
 * For usage notes, see DriverWrapper class, but use "jdbc:postgresql_lwgis:" as
 * JDBC url prefix and org.postgis.DriverWrapperLW as driver class.
 * 
 * @author Markus Schaber <markus.schaber@logix-tt.com>
 * @see DriverWrapper
 */
public class DriverWrapperLW extends DriverWrapper {

    public static final String POSTGIS_LWPROTOCOL = "jdbc:postgresql_lwgis:";
    public static final String REVISIONLW = "$Revision$";

    /**
     * Default constructor.
     */
    public DriverWrapperLW() throws SQLException {
        super();
    }

    static {
        try {
            // Try to register ourself to the DriverManager
            java.sql.DriverManager.registerDriver(new DriverWrapperLW());
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Error registering PostGIS LW Wrapper Driver", e);
        }
    }

    protected String getProtoString() {
        return POSTGIS_LWPROTOCOL;
    }

    protected boolean useLW(Connection result) {
        return true;
    }

    /**
     * Returns our own CVS version plus postgres Version
     */
    public static String getVersion() {
        return "PostGisWrapperLW " + REVISIONLW + ", wrapping " + Driver.getVersion();
    }
}
