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

public abstract class ByteSetter {

    /**
     * Set a byte.
     * 
     */
    public abstract void set(byte b, int index);

    public static class BinaryByteSetter extends ByteSetter {
        private byte[] array;

        public BinaryByteSetter(int length) {
            this.array = new byte[length];
        }

        public void set(byte b, int index) {
            array[index] = b; // mask out sign-extended bits.
        }

        public byte[] result() {
            return array;
        }
        
        public String toString() {
            char[] arr = new char[array.length];
            for (int i=0; i<array.length; i++) {
                arr[i] = (char)(array[i]&0xFF);
            }
            return new String(arr);
        }
    }

    public static class StringByteSetter extends ByteSetter {
        protected static final char[] hextypes = "0123456789ABCDEF".toCharArray();
        private char[] rep;

        public StringByteSetter(int length) {
            this.rep = new char[length * 2];
        }

        public void set(byte b, int index) {
            index *= 2;
            rep[index] = hextypes[(b >>> 4) & 0xF];
            rep[index + 1] = hextypes[b & 0xF];
        }

        public char[] resultAsArray() {
            return rep;
        }

        public String result() {
            return new String(rep);
        }
        
        public String toString() {
            return new String(rep);
        }
    }
}
