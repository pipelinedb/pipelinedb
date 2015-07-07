package org.postgis.jts;

import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.geom.PathIterator;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;

public class JTSShape implements Shape {
    static GeometryFactory fac = new GeometryFactory();

    Geometry geom;

    final static LinearRing[] NOSHELLS = {};

    public JTSShape(Geometry _geom) {
        this.geom = _geom;
    }
    
    public JTSShape(JtsGeometry _geom) {
        this(_geom.getGeometry());
    }
    
    public boolean contains(Point2D p) {
        return contains(p.getX(), p.getY());
    }

    public boolean contains(double x, double y) {
        Coordinate c = new Coordinate(x, y);
        Point p = fac.createPoint(c);
        return geom.contains(p);
    }

    public boolean contains(Rectangle2D r) {
        return contains(r.getMinX(), r.getMinY(), r.getWidth(), r.getHeight());
    }

    public boolean contains(double x, double y, double w, double h) {
        Polygon p = createRect(x, y, w, h);
        return geom.contains(p);
    }

    protected Polygon createRect(double x, double y, double w, double h) {
        double[] arr = { x, y, x + w, y, x + w, y + h, x, y + h, x, y };
        PackedCoordinateSequence shell = new PackedCoordinateSequence.Double(arr, 2);
        Polygon p = fac.createPolygon(fac.createLinearRing(shell), NOSHELLS);
        return p;
    }

    public Rectangle2D getBounds2D() {
        Envelope env = geom.getEnvelopeInternal();
        return new Rectangle2D.Double(env.getMinX(), env.getMaxX(), env.getWidth(), env.getHeight());
    }

    public Rectangle getBounds() {
        // We deal simple code for efficiency here, the getBounds() rounding
        // rules are ugly...
        return getBounds2D().getBounds();
    }

    public PathIterator getPathIterator(AffineTransform at) {
        return getPathIterator(geom, at);
    }

    public PathIterator getPathIterator(AffineTransform at, double flatness) {
        // we don't have much work here, as we only have linear segments, no
        // "flattening" necessary.
        return getPathIterator(at);
    }

    public boolean intersects(Rectangle2D r) {
        return intersects(r.getMinX(), r.getMinY(), r.getWidth(), r.getHeight());
    }

    public boolean intersects(double x, double y, double w, double h) {
        Polygon p = createRect(x, y, w, h);
        return geom.intersects(p);
    }

    public static GeometryPathIterator getPathIterator(Geometry geometry, AffineTransform _at) {
        if (geometry instanceof Point) {
            return new PointPathIterator((Point) geometry, _at);
        } else if (geometry instanceof LineString) {
            return new LineStringPathIterator((LineString) geometry, _at);
        } else if (geometry instanceof Polygon) {
            return new PolygonPathIterator((Polygon) geometry, _at);
        } else {
            return new GeometryCollectionPathIterator((GeometryCollection) geometry, _at);
        }
    }

    public static abstract class GeometryPathIterator implements PathIterator {

        protected final AffineTransform at;
        protected int index=0;

        GeometryPathIterator(AffineTransform _at) {
            this.at = _at;
        }

        public final int getWindingRule() {
            return PathIterator.WIND_EVEN_ODD;
        }

        public void next() {
            index++;
        }
    }
    
    public static class PointPathIterator extends GeometryPathIterator {
        final Point p;

        public PointPathIterator(Point _p, AffineTransform _at) {
            super(_at);
            p = _p;
        }

        public int currentSegment(float[] coords) {
            switch (index) {
            case 0:
                coords[0] = (float) p.getX();
                coords[1] = (float) p.getY();
                at.transform(coords, 0, coords, 0, 1);
                return PathIterator.SEG_MOVETO;
            case 1:
                return PathIterator.SEG_CLOSE;
            default:
                throw new IllegalStateException();
            }
        }

        public int currentSegment(double[] coords) {
            switch (index) {
            case 0:
                coords[0] = p.getX();
                coords[1] = p.getY();
                at.transform(coords, 0, coords, 0, 1);
                return PathIterator.SEG_MOVETO;
            case 1:
                return PathIterator.SEG_CLOSE;
            default:
                throw new IllegalStateException();
            }
        }

        public boolean isDone() {
            return index > 1;
        }
    }

    public static class LineStringPathIterator extends GeometryPathIterator {
        CoordinateSequence cs;

        final boolean isRing;

        public LineStringPathIterator(LineString ls, AffineTransform _at) {
            super(_at);
            cs = ls.getCoordinateSequence();
            isRing = ls instanceof LinearRing;
        }

        /** only to be called from PolygonPathIterator subclass */
        protected void reInit(CoordinateSequence _cs) {
            cs = _cs;
            index=0;
        }
        
        public int currentSegment(float[] coords) {
            if (index == 0) {
                coords[0] = (float) cs.getOrdinate(index, 0);
                coords[1] = (float) cs.getOrdinate(index, 1);
                at.transform(coords, 0, coords, 0, 1);
                return PathIterator.SEG_MOVETO;
            } else if (index < cs.size()) {
                coords[0] = (float) cs.getOrdinate(index, 0);
                coords[1] = (float) cs.getOrdinate(index, 1);
                at.transform(coords, 0, coords, 0, 1);
                return PathIterator.SEG_LINETO;
            } else if (isRing && index == cs.size()) {
                return PathIterator.SEG_CLOSE;
            } else {
                throw new IllegalStateException();
            }
        }

        public int currentSegment(double[] coords) {
            if (index == 0) {
                coords[0] = cs.getOrdinate(index, 0);
                coords[1] = cs.getOrdinate(index, 1);
                at.transform(coords, 0, coords, 0, 1);
                return PathIterator.SEG_MOVETO;
            } else if (index < cs.size()) {
                coords[0] = cs.getOrdinate(index, 0);
                coords[1] = cs.getOrdinate(index, 1);
                at.transform(coords, 0, coords, 0, 1);
                return PathIterator.SEG_LINETO;
            } else if (isRing && index == cs.size()) {
                return PathIterator.SEG_CLOSE;
            } else {
                throw new IllegalStateException();
            }
        }

        public boolean isDone() {
            return isRing ? index > cs.size() : index >= cs.size();
        }
    }
    
    public static class PolygonPathIterator extends LineStringPathIterator {
        final Polygon pg;
        int outerindex=-1;
        
        public PolygonPathIterator(Polygon _pg, AffineTransform _at) {
            super(_pg.getExteriorRing() ,_at);
            pg=_pg;
            index = -1;
        }

        public boolean isDone() {
            return outerindex >= pg.getNumInteriorRing();
        }
        
        public void next() {
            super.next();
            if (super.isDone()) {
                outerindex++;
                if (outerindex < pg.getNumInteriorRing()) {
                    super.reInit(pg.getInteriorRingN(outerindex).getCoordinateSequence());
                }
            }
        }
    }

    public static class GeometryCollectionPathIterator extends GeometryPathIterator {
        final GeometryCollection coll;
        GeometryPathIterator current;

        public GeometryCollectionPathIterator(GeometryCollection _coll, AffineTransform _at) {
            super(_at);
            coll = _coll;
            current = getPathIterator(coll.getGeometryN(index), _at);
        }

        public boolean isDone() {
            return index > coll.getNumGeometries();
        }
        
        public void next() {
            current.next();
            if (current.isDone()) {
                index++;
                if (index < coll.getNumGeometries()) {
                    current = getPathIterator(coll.getGeometryN(index), at);
                }
            }
        }

        public int currentSegment(float[] coords) {
            return current.currentSegment(coords);
        }

        public int currentSegment(double[] coords) {
            return current.currentSegment(coords);
        }
    }
}
