/*
 * ValueGetter.java
 * 
 * PostGIS extension for PostgreSQL JDBC driver - Binary Parser
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

package org.postgis.binary;

public abstract class ValueGetter {
    ByteGetter data;
    int position;
    public final byte endian;

    public ValueGetter(ByteGetter data, byte endian) {
        this.data = data;
        this.endian = endian;
    }

    /**
     * Get a byte, should be equal for all endians
     */
    public byte getByte() {
        return (byte) data.get(position++);
    }

    public int getInt() {
        int res = getInt(position);
        position += 4;
        return res;
    }

    public long getLong() {
        long res = getLong(position);
        position += 8;
        return res;
    }

    /** Get a 32-Bit integer */
    protected abstract int getInt(int index);

    /**
     * Get a long value. This is not needed directly, but as a nice side-effect
     * from GetDouble.
     */
    protected abstract long getLong(int index);

    /**
     * Get a double.
     */
    public double getDouble() {
        long bitrep = getLong();
        return Double.longBitsToDouble(bitrep);
    }

    public static class XDR extends ValueGetter {
        public static final byte NUMBER = 0;

        public XDR(ByteGetter data) {
            super(data, NUMBER);
        }

        protected int getInt(int index) {
            return (data.get(index) << 24) + (data.get(index + 1) << 16)
                    + (data.get(index + 2) << 8) + data.get(index + 3);
        }

        protected long getLong(int index) {
            return ((long) data.get(index) << 56) + ((long) data.get(index + 1) << 48)
                    + ((long) data.get(index + 2) << 40) + ((long) data.get(index + 3) << 32)
                    + ((long) data.get(index + 4) << 24) + ((long) data.get(index + 5) << 16)
                    + ((long) data.get(index + 6) << 8) + ((long) data.get(index + 7) << 0);
        }
    }

    public static class NDR extends ValueGetter {
        public static final byte NUMBER = 1;

        public NDR(ByteGetter data) {
            super(data, NUMBER);
        }

        protected int getInt(int index) {
            return (data.get(index + 3) << 24) + (data.get(index + 2) << 16)
                    + (data.get(index + 1) << 8) + data.get(index);
        }

        protected long getLong(int index) {
            return ((long) data.get(index + 7) << 56) + ((long) data.get(index + 6) << 48)
                    + ((long) data.get(index + 5) << 40) + ((long) data.get(index + 4) << 32)
                    + ((long) data.get(index + 3) << 24) + ((long) data.get(index + 2) << 16)
                    + ((long) data.get(index + 1) << 8) + ((long) data.get(index) << 0);
        }
    }
}
