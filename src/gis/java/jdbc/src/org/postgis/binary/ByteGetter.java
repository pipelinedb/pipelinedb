/*
 * ByteGetter.java
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

public abstract class ByteGetter {
    /**
     * Get a byte.
     * 
     * @return The result is returned as Int to eliminate sign problems when
     *         or'ing several values together.
     */
    public abstract int get(int index);

    public static class BinaryByteGetter extends ByteGetter {
        private byte[] array;

        public BinaryByteGetter(byte[] array) {
            this.array = array;
        }

        public int get(int index) {
            return array[index] & 0xFF; // mask out sign-extended bits.
        }
    }

    public static class StringByteGetter extends ByteGetter {
        private String rep;

        public StringByteGetter(String rep) {
            this.rep = rep;
        }

        public int get(int index) {
            index *= 2;
            int high = unhex(rep.charAt(index));
            int low = unhex(rep.charAt(index + 1));
            return (high << 4) + low;
        }

        public static byte unhex(char c) {
            if (c >= '0' && c <= '9') {
                return (byte) (c - '0');
            } else if (c >= 'A' && c <= 'F') {
                return (byte) (c - 'A' + 10);
            } else if (c >= 'a' && c <= 'f') {
                return (byte) (c - 'a' + 10);
            } else {
                throw new IllegalArgumentException("No valid Hex char " + c);
            }
        }
    }
}
