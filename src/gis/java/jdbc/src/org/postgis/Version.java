/*
 * Version.java
 * 
 * PostGIS extension for PostgreSQL JDBC driver - current version identification
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

import java.io.IOException;
import java.util.Properties;

/** Corresponds to the appropriate PostGIS that carried this source */
public class Version {
    /** We read our version information from this resource... */
    private static final String RESSOURCENAME = "org/postgis/version.properties";

    /** The major version */
    public static final int MAJOR;

    /** The minor version */
    public static final int MINOR;

    /**
     * The micro version, usually a number including possibly textual suffixes
     * like RC3.
     */
    public static final String MICRO;

    static {
        int major = -1;
        int minor = -1;
        String micro = null;
        try {
            ClassLoader loader = Version.class.getClassLoader();

            Properties prop = new Properties();

            try {
                prop.load(loader.getResourceAsStream(RESSOURCENAME));
            } catch (IOException e) {
                throw new ExceptionInInitializerError("Error initializing PostGIS JDBC version! Cause: Ressource "
                        + RESSOURCENAME + " cannot be read! " + e.getMessage());
            } catch (NullPointerException e) {
                throw new ExceptionInInitializerError("Error initializing PostGIS JDBC version! Cause: Ressource "
                        + RESSOURCENAME + " not found! " + e.getMessage());
            }

            try {
                major = Integer.parseInt(prop.getProperty("REL_MAJOR_VERSION"));
            } catch (NullPointerException e) {
                throw new ExceptionInInitializerError(
                        "Error initializing PostGIS JDBC version! Missing REL_MAJOR_VERSION! " + e.getMessage());
            } catch (NumberFormatException e) {
                throw new ExceptionInInitializerError(
                        "Error initializing PostGIS JDBC version! Error parsing REL_MAJOR_VERSION! " + e.getMessage());
            }

            try {
                minor = Integer.parseInt(prop.getProperty("REL_MINOR_VERSION"));
            } catch (NullPointerException e) {
                throw new ExceptionInInitializerError(
                        "Error initializing PostGIS JDBC version! Missing REL_MINOR_VERSION! " + e.getMessage());
            } catch (NumberFormatException e) {
                throw new ExceptionInInitializerError(
                        "Error initializing PostGIS JDBC version! Error parsing REL_MINOR_VERSION! " + e.getMessage());
            }

            micro = prop.getProperty("REL_MICRO_VERSION");
            if (micro == null) {
                throw new ExceptionInInitializerError(
                        "Error initializing PostGIS JDBC version! Missing REL_MICRO_VERSION! ");
            }
        } finally {
            MAJOR = major;
            MINOR = minor;
            MICRO = micro;
        }
    }

    /** Full version for human reading - code should use the constants above */
    public static final String FULL = "PostGIS JDBC V" + MAJOR + "." + MINOR + "." + MICRO;

    public static void main(String[] args) {
        System.out.println(FULL);
    }

    public static String getFullVersion() {
        return FULL;
    }
}
