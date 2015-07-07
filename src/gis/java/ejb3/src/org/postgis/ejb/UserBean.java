/*
 * UserBean.java
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Resource;
import javax.annotation.security.RunAs;
import javax.ejb.EJBException;
import javax.ejb.Stateless;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

import org.jboss.annotation.security.SecurityDomain;

@Stateless
@WebService(
		  name = "EndpointInterface", 
		  targetNamespace = "http://org.postgis/ejb/UserBean", 
		  serviceName = "PeopleFinder")
@SOAPBinding(style = SOAPBinding.Style.RPC)
public class UserBean implements UserBeanRemote{
	@PersistenceContext(unitName="People") private EntityManager entityManager;

	@Resource(mappedName = "java:/ConnectionFactory")
	private QueueConnectionFactory connectionFactory;
	
	@Resource(mappedName = "queue/ingestQueue")
	private Queue queue;


	@WebMethod
	public void ingest(@WebParam(name = "name") String name,@WebParam(name = "surname") String surname,@WebParam(name = "lat") double lat, @WebParam(name = "lon") double lon){
		// place message on a queue
		try {
			QueueConnection qConn = connectionFactory.createQueueConnection();
			QueueSession qSession = qConn.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
			QueueSender qSender = qSession.createSender(queue);

			// create a message
			MapMessage message = qSession.createMapMessage();
			message.setStringProperty("MessageFormat", "Person");
			message.setString("NAME", name);
			message.setString("SURNAME", surname);
			message.setDouble("LAT", lat);
			message.setDouble("LON", lon);
			qSender.send(message);
			qSession.close();
			qConn.close();
		} catch (JMSException e) {
			throw new EJBException(e.getMessage());
		}

	}


	@WebMethod
	@WebResult(name="positions")
	public String[] findPerson(@WebParam(name = "name") String name, @WebParam(name = "surname") String surname)
	{
		Query query = entityManager.createNamedQuery("findPerson");
		query.setParameter("name", name);
		query.setParameter("surname", surname);
		List list = query.getResultList();
		
		if (list != null)
		{
			Iterator itr = list.iterator();
			ArrayList<String> resultList = new ArrayList<String>();
			
			while (itr.hasNext())
			{
				PersonEntity person = (PersonEntity) itr.next();
				resultList.add(person.getLocation().getValue() + "," + person.getDate() + "\r\n");
			}
			
			String[] result = (String[])(resultList.toArray(new String[resultList.size()]));
			return result;
		}
		else
		{
			return null;
		}
		
	}

}
