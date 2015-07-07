/*
 * UserBeanRemote.java
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
package org.postgis.ejb;

import java.rmi.RemoteException;

import javax.ejb.Remote;
import javax.jws.WebParam;

@Remote
public interface UserBeanRemote {
	public void ingest(String name, String surname, double lat, double lon) throws RemoteException;
	public String[] findPerson(String name, String surname) throws RemoteException;
}
