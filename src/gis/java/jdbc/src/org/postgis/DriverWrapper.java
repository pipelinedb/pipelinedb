/*
 * DriverWrapper.java
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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.postgresql.Driver;
import org.postgresql.PGConnection;

/**
 * DriverWrapper
 * 
 * Wraps the PostGreSQL Driver to transparently add the PostGIS Object Classes.
 * This avoids the need of explicit addDataType() calls from the driver users
 * side.
 * 
 * This method currently works with J2EE DataSource implementations, and with
 * DriverManager framework.
 * 
 * Simply replace the "jdbc:postgresql:" with a "jdbc:postgresql_postGIS:" in
 * the jdbc URL.
 * 
 * When using the drivermanager, you need to initialize DriverWrapper instead of
 * (or in addition to) org.postgresql.Driver. When using a J2EE DataSource
 * implementation, set the driver class property in the datasource config, the
 * following works for jboss:
 * 
 * <code>
 * &lt;driver-class&gt;org.postgis.DriverWrapper&lt;/driver-class&gt;
 * </code>
 * If you don't like or want to use the DriverWrapper, you have two
 * alternatives, see the README file.
 * 
 * Also note that the addDataType() methods known from earlier pgjdbc versions
 * are deprecated in pgjdbc 8.0, see the commented code variants in the
 * addGisTypes() method.
 * 
 * This wrapper always uses EWKT as canonical text representation, and thus
 * works against PostGIS 1.x servers as well as 0.x (tested with 0.8, 0.9 and
 * 1.0).
 * 
 * @author Markus Schaber <markus.schaber@logix-tt.com>
 * @see DriverWrapperLW
 * @see DriverWrapperAutoprobe
 */
public class DriverWrapper extends Driver {

    protected static final Logger logger = Logger.getLogger("org.postgis.DriverWrapper");
    
    public static final String POSTGRES_PROTOCOL = "jdbc:postgresql:";
    public static final String POSTGIS_PROTOCOL = "jdbc:postgresql_postGIS:";
    public static final String REVISION = "$Revision$";
    protected static TypesAdder ta72 = null;
    protected static TypesAdder ta74 = null;
    protected static TypesAdder ta80 = null;

    protected TypesAdder typesAdder;

    /**
     * Default constructor.
     * 
     * This also loads the appropriate TypesAdder for our SQL Driver instance.
     * 
     * @throws SQLException
     */
    public DriverWrapper() throws SQLException {
        super();
        typesAdder = getTypesAdder(this);
        // The debug method is @since 7.2
        if (super.getMajorVersion() > 8 || super.getMinorVersion() > 1) {
            logger.fine(this.getClass().getName() + " loaded TypesAdder: "
                    + typesAdder.getClass().getName());
        }
    }

    protected static TypesAdder getTypesAdder(Driver d) throws SQLException {
        if (d.getMajorVersion() == 7) {
            if (d.getMinorVersion() >= 3) {
                if (ta74 == null) {
                    ta74 = loadTypesAdder("74");
                }
                return ta74;
            } else {
                if (ta72 == null) {
                    ta72 = loadTypesAdder("72");
                }
                return ta72;
            }
        } else {
            if (ta80 == null) {
                ta80 = loadTypesAdder("80");
            }
            return ta80;
        }
    }

    private static TypesAdder loadTypesAdder(String version) throws SQLException {
        try {
            Class klass = Class.forName("org.postgis.DriverWrapper$TypesAdder" + version);
            return (TypesAdder) klass.newInstance();
        } catch (Exception e) {
            throw new SQLException("Cannot create TypesAdder instance! " + e.getMessage());
        }
    }

    static {
        try {
            // Try to register ourself to the DriverManager
            java.sql.DriverManager.registerDriver(new DriverWrapper());
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Error registering PostGIS Wrapper Driver", e);
        }
    }

    /**
     * Creates a postgresql connection, and then adds the PostGIS data types to
     * it calling addpgtypes()
     * 
     * @param url the URL of the database to connect to
     * @param info a list of arbitrary tag/value pairs as connection arguments
     * @return a connection to the URL or null if it isnt us
     * @exception SQLException if a database access error occurs
     * 
     * @see java.sql.Driver#connect
     * @see org.postgresql.Driver
     */
    public java.sql.Connection connect(String url, Properties info) throws SQLException {
        url = mangleURL(url);
        Connection result = super.connect(url, info);
        typesAdder.addGT(result, useLW(result));
        return result;
    }

    /**
     * Do we have HexWKB as well known text representation - to be overridden by
     * subclasses.
     */
    protected boolean useLW(Connection result) {
        if (result == null) {
            throw new IllegalArgumentException("null is no valid parameter");
        }
        return false;
    }

    /**
     * Check whether the driver thinks he can handle the given URL.
     * 
     * @see java.sql.Driver#acceptsURL
     * @param url the URL of the driver
     * @return true if this driver accepts the given URL
     * @exception SQLException Passed through from the underlying PostgreSQL
     *                driver, should not happen.
     */
    public boolean acceptsURL(String url) throws SQLException {
        try {
            url = mangleURL(url);
        } catch (SQLException e) {
            return false;
        }
        return super.acceptsURL(url);
    }

    /**
     * Returns our own CVS version plus postgres Version
     */
    public static String getVersion() {
        return "PostGisWrapper " + REVISION + ", wrapping " + Driver.getVersion();
    }

    /*
     * Here follows the addGISTypes() stuff. This is a little tricky because the
     * pgjdbc people had several, partially incompatible API changes during 7.2
     * and 8.0. We still want to support all those releases, however.
     * 
     */
    /**
     * adds the JTS/PostGIS Data types to a PG 7.3+ Connection. If you use
     * PostgreSQL jdbc drivers V8.0 or newer, those methods are deprecated due
     * to some class loader problems (but still work for now), and you may want
     * to use the method below instead.
     * 
     * @throws SQLException
     * 
     */
    public static void addGISTypes(PGConnection pgconn) throws SQLException {
        loadTypesAdder("74").addGT((Connection) pgconn, false);
    }

    /**
     * adds the JTS/PostGIS Data types to a PG 8.0+ Connection.
     */
    public static void addGISTypes80(PGConnection pgconn) throws SQLException {
        loadTypesAdder("80").addGT((Connection) pgconn, false);
    }

    /**
     * adds the JTS/PostGIS Data types to a PG 7.2 Connection.
     * 
     * @throws SQLException
     */
    public static void addGISTypes72(org.postgresql.PGConnection pgconn) throws SQLException {
        loadTypesAdder("72").addGT((Connection) pgconn, false);
    }

    /**
     * Mangles the PostGIS URL to return the original PostGreSQL URL
     */
    protected String mangleURL(String url) throws SQLException {
        String myProgo = getProtoString();
        if (url.startsWith(myProgo)) {
            return POSTGRES_PROTOCOL + url.substring(myProgo.length());
        } else {
            throw new SQLException("Unknown protocol or subprotocol in url " + url);
        }
    }

    protected String getProtoString() {
        return POSTGIS_PROTOCOL;
    }

    /** Base class for the three typewrapper implementations */
    protected abstract static class TypesAdder {
        public final void addGT(java.sql.Connection conn, boolean lw) throws SQLException {
            if (lw) {
                addBinaryGeometries(conn);
            } else {
                addGeometries(conn);
            }
            addBoxen(conn);
        }

        public abstract void addGeometries(Connection conn) throws SQLException;

        public abstract void addBoxen(Connection conn) throws SQLException;

        public abstract void addBinaryGeometries(Connection conn) throws SQLException;
    }

    /** addGISTypes for V7.3 and V7.4 pgjdbc */
    protected static final class TypesAdder74 extends TypesAdder {
        public void addGeometries(Connection conn) throws SQLException {
            PGConnection pgconn = (PGConnection) conn;
            pgconn.addDataType("geometry", org.postgis.PGgeometry.class);
        }

        public void addBoxen(Connection conn) throws SQLException {
            PGConnection pgconn = (PGConnection) conn;
            pgconn.addDataType("box3d", org.postgis.PGbox3d.class);
            pgconn.addDataType("box2d", org.postgis.PGbox2d.class);
        }

        public void addBinaryGeometries(Connection conn) throws SQLException {
            PGConnection pgconn = (PGConnection) conn;
            pgconn.addDataType("geometry", org.postgis.PGgeometryLW.class);
        }
    }

    /** addGISTypes for V7.2 pgjdbc */
    protected static class TypesAdder72 extends TypesAdder {
        public void addGeometries(Connection conn) throws SQLException {
            org.postgresql.PGConnection pgconn = (org.postgresql.PGConnection) conn;
            pgconn.addDataType("geometry", org.postgis.PGgeometry.class);
        }

		public void addBoxen(Connection conn) throws SQLException {
            org.postgresql.PGConnection pgconn = (org.postgresql.PGConnection) conn;
            pgconn.addDataType("box3d", org.postgis.PGbox3d.class);
            pgconn.addDataType("box2d", org.postgis.PGbox2d.class);
        }

        public void addBinaryGeometries(Connection conn) throws SQLException {
            org.postgresql.PGConnection pgconn = (org.postgresql.PGConnection) conn;
            pgconn.addDataType("geometry", org.postgis.PGgeometryLW.class);
        }
    }

    /** addGISTypes for V8.0 (and hopefully newer) pgjdbc */
    protected static class TypesAdder80 extends TypesAdder {
        public void addGeometries(Connection conn) throws SQLException {
            PGConnection pgconn = (PGConnection) conn;
            pgconn.addDataType("geometry", org.postgis.PGgeometry.class);
        }

        public void addBoxen(Connection conn) throws SQLException {
            PGConnection pgconn = (PGConnection) conn;
            pgconn.addDataType("box3d", org.postgis.PGbox3d.class);
            pgconn.addDataType("box2d", org.postgis.PGbox2d.class);
        }

        public void addBinaryGeometries(Connection conn) throws SQLException {
            PGConnection pgconn = (PGConnection) conn;
            pgconn.addDataType("geometry", org.postgis.PGgeometryLW.class);
        }
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
