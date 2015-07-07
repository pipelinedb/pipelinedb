/*
 * Test.java
 * 
 * PostGIS extension for PostgreSQL JDBC driver - example and test classes
 * 
 * (C) 2004 Paul Ramsey, pramsey@refractions.net
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestServer {

	public static void main(String[] args) {
		if (args.length != 4 && args.length != 3) {
			System.err
					.println("Usage: java examples/TestServer dburl user pass [tablename]");
			System.err.println();
			System.err.println("dburl has the following format:");
			System.err.println("jdbc:postgresql://HOST:PORT/DATABASENAME");
			System.err.println("tablename is 'jdbc_test' by default.");
			System.exit(1);
		}

		Connection conn;

		String dburl = args[0];
		String dbuser = args[1];
		String dbpass = args[2];

		String dbtable = "jdbc_test";

		String dropSQL = "drop table " + dbtable;
		String createSQL = "create table " + dbtable
				+ " (geom geometry, id int4)";
		String insertPointSQL = "insert into " + dbtable
				+ " values ('POINT (10 10 10)',1)";
		String insertPolygonSQL = "insert into " + dbtable
				+ " values ('POLYGON ((0 0 0,0 10 0,10 10 0,10 0 0,0 0 0))',2)";

		try {

			System.out.println("Creating JDBC connection...");
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection(dburl, dbuser, dbpass);
			System.out.println("Adding geometric type entries...");

			((org.postgresql.PGConnection) conn).addDataType("geometry",
					org.postgis.PGgeometry.class);
			((org.postgresql.PGConnection) conn).addDataType("box3d",
					org.postgis.PGbox3d.class);

			Statement s = conn.createStatement();
			System.out.println("Creating table with geometric types...");
			// table might not yet exist
			try {
				s.execute(dropSQL);
			} catch (Exception e) {
				System.out.println("Error dropping old table: "
						+ e.getMessage());
			}
			s.execute(createSQL);
			System.out.println("Inserting point...");
			s.execute(insertPointSQL);
			System.out.println("Inserting polygon...");
			s.execute(insertPolygonSQL);
			System.out.println("Done.");
			s = conn.createStatement();
			System.out.println("Querying table...");
			ResultSet r = s.executeQuery("select asText(geom),id from "
					+ dbtable);
			while (r.next()) {
				Object obj = r.getObject(1);
				int id = r.getInt(2);
				System.out.println("Row " + id + ":");
				System.out.println(obj.toString());
			}
			s.close();
			conn.close();
		} catch (Exception e) {
			System.err.println("Aborted due to error:");
			e.printStackTrace();
			System.exit(1);
		}
	}
}
