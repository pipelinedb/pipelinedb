/*
 * IngestMDB.java
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
package org.postgis.ejb.mdb;

import java.util.Date;

import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.postgis.Point;
import org.postgis.ejb.PersonEntity;

@MessageDriven(activationConfig={
		@ActivationConfigProperty(
				propertyName="destinationType",
				propertyValue="javax.jms.Queue"),
		@ActivationConfigProperty(
				propertyName="destination", 
				propertyValue="queue/ingestQueue"),
		@ActivationConfigProperty(
				propertyName="messageSelector",
				propertyValue="MessageFormat ='Person'"),
		@ActivationConfigProperty(
				propertyName="acknowledgeMode",
				propertyValue="Auto-acknowledge")
})
/**
 * implements a listener interface to ingest people data
 */
public class IngestMDB implements MessageListener {
    
	public static final String NAME = "NAME";
	public static final String SURNAME = "SURNAME";
	public static final String LATITUDE = "LAT";
	public static final String LONGITUDE = "LON";
	
	@PersistenceContext(unitName="People") private EntityManager entityManager;

	
	/**
	 * Implements a message listener for Person ingest requests
	 * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
	 */
	public void onMessage(Message msg) {
		if (msg instanceof MapMessage)
		{
			try {
				MapMessage m = (MapMessage)msg;
				String name = m.getString(IngestMDB.NAME);
				String surname = m.getString(IngestMDB.SURNAME);
				Double lat = m.getDouble(IngestMDB.LATITUDE);
				Double lon = m.getDouble(IngestMDB.LONGITUDE);
				
				PersonEntity person = new PersonEntity();
				person.setName(name);
				person.setSurname(surname);
				person.setLocation(new Point(lon, lat));
				person.setDate(new Date());
				entityManager.persist(person);
				
				// for tutorial info
				System.out.println("INGESTED " + name + " " + surname + " into PostGIS");
			} catch (JMSException e) {
				e.printStackTrace();
			}
			
			
		}
		
	}

}
