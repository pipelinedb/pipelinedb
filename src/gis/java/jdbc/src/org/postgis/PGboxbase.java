/*
 * PGboxbase.java
 * 
 * PostGIS extension for PostgreSQL JDBC driver - bounding box model
 * 
 * 
 * (C) 2004 Paul Ramsey, pramsey@refractions.net
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

import org.postgresql.util.PGobject;
import org.postgresql.util.PGtokenizer;

import java.sql.SQLException;

/*
 * Updates Oct 2002 - data members made private - getLLB() and getURT() methods
 * added
 */

public abstract class PGboxbase extends PGobject {
    /* JDK 1.5 Serialization */
    private static final long serialVersionUID = 0x100;

    /**
     * The lower left bottom corner of the box.
     */
    protected Point llb;

    /**
     * The upper right top corner of the box.
     */
    protected Point urt;

    /**
     * The Prefix we have in WKT rep.
     * 
     * I use an abstract method here so we do not need to replicate the String
     * object in every instance.
     * 
     */
    public abstract String getPrefix();

    /**
     * The Postgres type we have (same construct as getPrefix())
     */
    public abstract String getPGtype();

    public PGboxbase() {
        this.setType(getPGtype());
    }

    public PGboxbase(Point llb, Point urt) {
        this();
        this.llb = llb;
        this.urt = urt;
    }

    public PGboxbase(String value) throws SQLException {
        this();
        setValue(value);
    }

    public void setValue(String value) throws SQLException {
        int srid = Geometry.UNKNOWN_SRID;
        value = value.trim();
        if (value.startsWith("SRID=")) {
            String[] temp = PGgeometry.splitSRID(value);
            value = temp[1].trim();
            srid = Geometry.parseSRID(Integer.parseInt(temp[0].substring(5)));
        }
        String myPrefix = getPrefix();
        if (value.startsWith(myPrefix)) {
            value = value.substring(myPrefix.length()).trim();
        }
        PGtokenizer t = new PGtokenizer(PGtokenizer.removePara(value), ',');
        llb = new Point(t.getToken(0));
        urt = new Point(t.getToken(1));
        if (srid != Geometry.UNKNOWN_SRID) {
            llb.setSrid(srid);
            urt.setSrid(srid);
        }
    }

    public String getValue() {
        StringBuffer sb = new StringBuffer();
        outerWKT(sb);
        return sb.toString();
    }

    private void outerWKT(StringBuffer sb) {
        sb.append(getPrefix());
        sb.append('(');
        llb.innerWKT(sb);
        sb.append(',');
        urt.innerWKT(sb);
        sb.append(')');
    }

    /**
     * Unlike geometries, toString() does _not_ contain the srid, as server-side
     * PostGIS cannot parse this.
     */
    public String toString() {
        return getValue();
    }

    /** Returns the lower left bottom corner of the box as a Point object */
    public Point getLLB() {
        return llb;
    }

    /** Returns the upper right top corner of the box as a Point object */
    public Point getURT() {
        return urt;
    }

    public boolean equals(Object other) {
        if (other instanceof PGboxbase) {
            PGboxbase otherbox = (PGboxbase) other;
            return (compareLazyDim(this.llb, otherbox.llb) && compareLazyDim(this.urt, otherbox.urt));
        }
        return false;
    }

    /**
     * Compare two coordinates with lazy dimension checking.
     * 
     * As the Server always returns Box3D with three dimensions, z==0 equals
     * dimensions==2
     * 
     */
    protected static boolean compareLazyDim(Point first, Point second) {
        return first.x == second.x
                && first.y == second.y
                && (((first.dimension == 2 || first.z == 0.0) && (second.dimension == 2 || second.z == 0)) || (first.z == second.z));
    }

    public Object clone() {
        PGboxbase obj = newInstance();
        obj.llb = this.llb;
        obj.urt = this.urt;
        obj.setType(type);
        return obj;
    }

    /**
     * We could have used this.getClass().newInstance() here, but this forces us
     * dealing with InstantiationException and IllegalAccessException. Due to
     * the PGObject.clone() brokennes that does not allow clone() to throw
     * CloneNotSupportedException, we cannot even pass this exceptions down to
     * callers in a sane way.
     */
    protected abstract PGboxbase newInstance();
}
