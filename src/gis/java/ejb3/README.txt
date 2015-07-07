(This code was contributed by Norman Barker <norman.barker@gmail.com>)

Spatial EJB3


Spatial EJB3 is a quick investigation to see if it is possible to integrate the
Java 5 annotation approach to mark a property of an object as spatial and to
delegate to the EJB3 persistence model to store and retrieve this data. 

The project utilises JBoss and PostGIS, future iterations will look to remove
the dependency on JBoss and Hibernate to incorporate other Application
Services. 

Since it is useful to display screenshots in a tutorial this has been written
as an Openoffice Document. For easy viewing and printing, a PDF version is
also available.


COMPILING:

Run "ant" to compile.

The postgis.jar, pgjdbc.jar and the other needed libs have to 
be put in the lib/ subdirectory.

If your JBOSS is not installed in C:\jboss-4.0.4.GA, fix the
path in the build.xml.
