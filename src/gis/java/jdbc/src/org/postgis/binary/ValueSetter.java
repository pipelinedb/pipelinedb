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

public abstract class ValueSetter {
    ByteSetter data;
    int position=0;
    public final byte endian;

    public ValueSetter(ByteSetter data, byte endian) {
        this.data = data;
        this.endian = endian;
    }

    /**
     * Get a byte, should be equal for all endians
     */
    public void setByte(byte value) {
        data.set(value, position);
        position += 1;
    }

    public void setInt(int value) {
        setInt(value, position);
        position += 4;
    }

    public void setLong(long value) {
        setLong(value, position);
        position += 8;
    }

    /** Get a 32-Bit integer */
    protected abstract void setInt(int value, int index);

    /**
     * Get a long value. This is not needed directly, but as a nice side-effect
     * from GetDouble.
     */
    protected abstract void setLong(long data, int index);

    /**
     * Get a double.
     */
    public void setDouble(double data) {
        long bitrep = Double.doubleToLongBits(data);
        setLong(bitrep);
    }

    public String toString() {
        String name = getClass().getName();
        int pointpos = name.lastIndexOf('.');
        String klsName = name.substring(pointpos+1);
        return klsName+"('"+(data==null?"NULL":data.toString()+"')");
    }
    
    public static class XDR extends ValueSetter {
        public static final byte NUMBER = 0;

        public XDR(ByteSetter data) {
            super(data, NUMBER);
        }

        protected void setInt(int value, int index) {
            data.set((byte) (value >>> 24), index);
            data.set((byte) (value >>> 16), index + 1);
            data.set((byte) (value >>> 8), index + 2);
            data.set((byte) value, index + 3);
        }

        protected void setLong(long value, int index) {
            data.set((byte) (value >>> 56), index);
            data.set((byte) (value >>> 48), index + 1);
            data.set((byte) (value >>> 40), index + 2);
            data.set((byte) (value >>> 32), index + 3);
            data.set((byte) (value >>> 24), index + 4);
            data.set((byte) (value >>> 16), index + 5);
            data.set((byte) (value >>> 8), index + 6);
            data.set((byte) value, index + 7);
        }
    }

    public static class NDR extends ValueSetter {
        public static final byte NUMBER = 1;

        public NDR(ByteSetter data) {
            super(data, NUMBER);
        }

        protected void setInt(int value, int index) {
            data.set((byte) (value >>> 24), index + 3);
            data.set((byte) (value >>> 16), index + 2);
            data.set((byte) (value >>> 8), index + 1);
            data.set((byte) value, index);
        }

        protected void setLong(long value, int index) {
            data.set((byte) (value >>> 56), index + 7);
            data.set((byte) (value >>> 48), index + 6);
            data.set((byte) (value >>> 40), index + 5);
            data.set((byte) (value >>> 32), index + 4);
            data.set((byte) (value >>> 24), index + 3);
            data.set((byte) (value >>> 16), index + 2);
            data.set((byte) (value >>> 8), index + 1);
            data.set((byte) value, index);
        }
    }
}
