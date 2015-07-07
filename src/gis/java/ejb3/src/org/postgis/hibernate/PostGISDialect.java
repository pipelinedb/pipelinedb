/*
 * PostGISDialect.java
 * 
 * PostGIS extension for PostgreSQL JDBC driver - EJB3 Tutorial
 * 
 * (C) 2006  Norman Barker <norman.barker@gmail.com>
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
package org.postgis.hibernate;

import java.sql.Types;

import org.hibernate.Hibernate;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.postgis.hibernate.GeometryType;

/**
 * @author nbarker
 *
 */
public class PostGISDialect extends PostgreSQLDialect{
	public static String NAMESPACE = "spatial.";
	

	public PostGISDialect()
	{
		super();
		registerColumnType(Types.BLOB, "geometry");
		registerFunction( PostGISDialect.NAMESPACE + "dimension", new StandardSQLFunction("dimension", Hibernate.INTEGER));
		registerFunction( PostGISDialect.NAMESPACE + "geometrytype", new StandardSQLFunction("geometrytype", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "srid", new StandardSQLFunction("srid", Hibernate.INTEGER));
		registerFunction( PostGISDialect.NAMESPACE + "envelope", new StandardSQLFunction("envelope", Hibernate.custom(GeometryType.class)));
		registerFunction( PostGISDialect.NAMESPACE + "astext", new StandardSQLFunction("astext", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "asbinary", new StandardSQLFunction("asbinary", Hibernate.BINARY));
		registerFunction( PostGISDialect.NAMESPACE + "isempty", new StandardSQLFunction("isempty", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "issimple", new StandardSQLFunction("issimple", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "boundary", new StandardSQLFunction("boundary", Hibernate.custom(GeometryType.class)));
		registerFunction( PostGISDialect.NAMESPACE + "equals", new StandardSQLFunction("equals", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "disjoint", new StandardSQLFunction("disjoint", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "intersects", new StandardSQLFunction("intersects", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "touches", new StandardSQLFunction("touches", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "crosses", new StandardSQLFunction("crosses", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "within", new StandardSQLFunction("within", Hibernate.BOOLEAN));
		registerFunction( PostGISDialect.NAMESPACE + "contains", new StandardSQLFunction("contains", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "overlaps", new StandardSQLFunction("overlaps", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "relate", new StandardSQLFunction("relate", Hibernate.STRING));
		registerFunction( PostGISDialect.NAMESPACE + "distance", new StandardSQLFunction("distance", Hibernate.DOUBLE));
		registerFunction( PostGISDialect.NAMESPACE + "buffer", new StandardSQLFunction("buffer", Hibernate.custom(GeometryType.class)));
		registerFunction( PostGISDialect.NAMESPACE + "convexhull", new StandardSQLFunction("convexhull", Hibernate.custom(GeometryType.class)));
		registerFunction( PostGISDialect.NAMESPACE + "intersection", new StandardSQLFunction("intersection", Hibernate.custom(GeometryType.class)));
		registerFunction( PostGISDialect.NAMESPACE + "union", new StandardSQLFunction("geomunion", Hibernate.custom(GeometryType.class)));
		registerFunction( PostGISDialect.NAMESPACE + "difference", new StandardSQLFunction("difference", Hibernate.custom(GeometryType.class)));
		registerFunction( PostGISDialect.NAMESPACE + "symdifference", new StandardSQLFunction("symdifference", Hibernate.custom(GeometryType.class)));
		registerFunction( PostGISDialect.NAMESPACE + "numgeometries", new StandardSQLFunction("numgeometries", Hibernate.custom(GeometryType.class)));
		registerFunction( PostGISDialect.NAMESPACE + "geometryn", new StandardSQLFunction("geometryn", Hibernate.INTEGER));
		registerFunction( PostGISDialect.NAMESPACE + "x", new StandardSQLFunction("x", Hibernate.DOUBLE));
		registerFunction( PostGISDialect.NAMESPACE + "y", new StandardSQLFunction("y", Hibernate.DOUBLE));
		registerFunction( PostGISDialect.NAMESPACE + "geometryfromewtk", new StandardSQLFunction("geometryfromewtk", Hibernate.custom(GeometryType.class)));
	}
}
