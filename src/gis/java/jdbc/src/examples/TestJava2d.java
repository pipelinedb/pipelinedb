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

import java.awt.*;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.awt.geom.AffineTransform;
import java.awt.geom.Rectangle2D;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.postgis.java2d.Java2DWrapper;

public class TestJava2d {
    private static final boolean DEBUG = true;

    public static final Shape[] SHAPEARRAY = new Shape[0];

    static {
        new Java2DWrapper(); // make shure our driver is initialized
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        if (args.length != 5) {
            System.err.println("Usage: java examples/TestJava2D dburl user pass tablename column");
            System.err.println();
            System.err.println("dburl has the following format:");
            System.err.println(Java2DWrapper.POSTGIS_PROTOCOL + "//HOST:PORT/DATABASENAME");
            System.err.println("tablename is 'jdbc_test' by default.");
            System.exit(1);
        }

        Shape[] geometries = read(args[0], args[1], args[2], "SELECT " + args[4] + " FROM "
                + args[3]);

        if (geometries.length == 0) {
            System.err.println("No geometries were found.");
            return;
        }

        System.err.println("Painting...");
        Frame window = new Frame("PostGIS java2D demo");

        Canvas CV = new GisCanvas(geometries);

        window.add(CV);

        window.setSize(500, 500);

        window.addWindowListener(new EventHandler());

        window.setVisible(true);
    }

    static Rectangle2D calcbbox(Shape[] geometries) {
        Rectangle2D bbox = geometries[0].getBounds2D();
        for (int i = 1; i < geometries.length; i++) {
            bbox = bbox.createUnion(geometries[i].getBounds2D());
        }
        return bbox;
    }

    private static Shape[] read(String dburl, String dbuser, String dbpass, String query)
            throws ClassNotFoundException, SQLException {
        ArrayList geometries = new ArrayList();
        System.out.println("Creating JDBC connection...");
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection(dburl, dbuser, dbpass);

        System.out.println("fetching geometries");
        ResultSet r = conn.createStatement().executeQuery(query);

        while (r.next()) {
            final Shape current = (Shape) r.getObject(1);
            if (current != null) {
                geometries.add(current);
            }
        }
        conn.close();
        return (Shape[]) geometries.toArray(SHAPEARRAY);
    }

    public static class GisCanvas extends Canvas {
        /** Keep java 1.5 compiler happy */
        private static final long serialVersionUID = 1L;

        final Rectangle2D bbox;
        final Shape[] geometries;

        public GisCanvas(Shape[] geometries) {
            this.geometries = geometries;
            this.bbox = calcbbox(geometries);
            setBackground(Color.GREEN);
        }

        public void paint(Graphics og) {
            Graphics2D g = (Graphics2D) og;

            final double scaleX = (super.getWidth() - 10) / bbox.getWidth();
            final double scaleY = (super.getHeight() - 10) / bbox.getHeight();

            AffineTransform at = new AffineTransform();
            at.translate(super.getX() + 5, super.getY() + 5);
            at.scale(scaleX, scaleY);
            at.translate(-bbox.getX(), -bbox.getY());

            if (DEBUG) {
                System.err.println();
                System.err.println("bbox:  " + bbox);
                System.err.println("trans: " + at);
                System.err.println("new:   " + at.createTransformedShape(bbox).getBounds2D());
                System.err.println("visual:" + super.getBounds());
            }
            for (int i = 0; i < geometries.length; i++) {
                g.setPaint(Color.BLUE);
                final Shape shape = at.createTransformedShape(geometries[i]);
                g.fill(shape);
                g.setPaint(Color.ORANGE);
                g.draw(shape);
            }
        }
    }

    public static class EventHandler implements WindowListener {

        public void windowActivated(WindowEvent e) {//
        }

        public void windowClosed(WindowEvent e) {//
        }

        public void windowClosing(WindowEvent e) {
            e.getWindow().setVisible(false);
            System.exit(0);
        }

        public void windowDeactivated(WindowEvent e) {//
        }

        public void windowDeiconified(WindowEvent e) {//
        }

        public void windowIconified(WindowEvent e) {//
        }

        public void windowOpened(WindowEvent e) {//
        }
    }
}
