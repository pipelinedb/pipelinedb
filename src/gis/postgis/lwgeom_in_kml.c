/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL

 * Copyright 2009 Oslandia
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

/**
* @file KML input routines.
* Ability to parse KML geometry fragment and to return an LWGEOM
* or an error message.
*
* KML version supported: 2.2.0
* Cf: <http://www.opengeospatial.org/standards/kml>
*
* Known limitations related to 3D:
*  - Not support kml:Model geometries
*  - Don't handle kml:extrude attribute
*
* Written by Olivier Courtin - Oslandia
*
**********************************************************************/


#include <libxml/tree.h>
#include <libxml/parser.h>
#include <errno.h>
#include <string.h>

#include "postgres.h"

#include "../postgis_config.h"
#include "lwgeom_pg.h"
#include "liblwgeom.h"



/*
TODO:
	- OGC:LonLat84_5773 explicit support (rather than EPSG:4326)
	- altitudeModeGroup relativeToGround Z Altitude
	  computation upon Geoid
*/


Datum geom_from_kml(PG_FUNCTION_ARGS);
static LWGEOM* parse_kml(xmlNodePtr xnode, bool *hasz);

#define KML_NS		((char *) "http://www.opengis.net/kml/2.2")


/**
 * Ability to parse KML geometry fragment and to return an LWGEOM
 * or an error message.
 */
PG_FUNCTION_INFO_V1(geom_from_kml);
Datum geom_from_kml(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom;
	LWGEOM *lwgeom, *hlwgeom;
	xmlDocPtr xmldoc;
	text *xml_input;
	int xml_size;
	char *xml;
	bool hasz=true;
	xmlNodePtr xmlroot=NULL;


	/* Get the KML stream */
	if (PG_ARGISNULL(0)) PG_RETURN_NULL();
	xml_input = PG_GETARG_TEXT_P(0);
	xml = text2cstring(xml_input);
	xml_size = VARSIZE(xml_input) - VARHDRSZ;

	/* Begin to Parse XML doc */
	xmlInitParser();
	xmldoc = xmlReadMemory(xml, xml_size, NULL, NULL, XML_PARSE_SAX1);
	if (!xmldoc || (xmlroot = xmlDocGetRootElement(xmldoc)) == NULL)
	{
		xmlFreeDoc(xmldoc);
		xmlCleanupParser();
		lwerror("invalid KML representation");
	}

	lwgeom = parse_kml(xmlroot, &hasz);

	/* Homogenize geometry result if needed */
	if (lwgeom->type == COLLECTIONTYPE)
	{
		hlwgeom = lwgeom_homogenize(lwgeom);
		lwgeom_release(lwgeom);
		lwgeom = hlwgeom;
	}

	lwgeom_add_bbox(lwgeom);

	/* KML geometries could be either 2 or 3D
	 *
	 * So we deal with 3D in all structures allocation, and flag hasz
	 * to false if we met once a missing Z dimension
	 * In this case, we force recursive 2D.
	 */
	if (!hasz)
	{
		LWGEOM *tmp = lwgeom_force_2d(lwgeom);
		lwgeom_free(lwgeom);
		lwgeom = tmp;
	}

	geom = geometry_serialize(lwgeom);
	lwgeom_free(lwgeom);

	xmlFreeDoc(xmldoc);
	xmlCleanupParser();

	PG_RETURN_POINTER(geom);
}


/**
 * Return false if current element namespace is not a KML one
 * Return true otherwise.
 */
static bool is_kml_namespace(xmlNodePtr xnode, bool is_strict)
{
	xmlNsPtr *ns, *p;

	ns = xmlGetNsList(xnode->doc, xnode);
	/*
	 * If no namespace is available we could return true anyway
	 * (because we work only on KML fragment, we don't want to
	 *  'oblige' to add namespace on the geometry root node)
	 */
	if (ns == NULL) return !is_strict;

        for (p=ns ; *p ; p++)
        {
                if ((*p)->href == NULL || (*p)->prefix == NULL ||
                     xnode->ns == NULL || xnode->ns->prefix == NULL) continue;

                if (!xmlStrcmp(xnode->ns->prefix, (*p)->prefix))
                {
                        if (!strcmp((char *) (*p)->href, KML_NS))
                        {
                                xmlFree(ns);
                                return true;
                        } else {
                                xmlFree(ns);
                                return false;
                        }
                }
        }

        xmlFree(ns);
        return !is_strict; /* Same reason here to not return false */;
}


/* Temporarily disabling unused function. */
#if 0
/**
 * Retrieve a KML propertie from a node or NULL otherwise
 * Respect namespaces if presents in the node element
 */
static xmlChar *kmlGetProp(xmlNodePtr xnode, xmlChar *prop)
{
	xmlChar *value;

	if (!is_kml_namespace(xnode, true))
		return xmlGetProp(xnode, prop);

	value = xmlGetNsProp(xnode, prop, (xmlChar *) KML_NS);

	/* In last case try without explicit namespace */
	if (value == NULL) value = xmlGetNoNsProp(xnode, prop);

	return value;
}
#endif


#if 0 /* unused */
/**
 * Parse a string supposed to be a double
 */
static double parse_kml_double(char *d, bool space_before, bool space_after)
{
	char *p;
	int st;
	enum states
	{
		INIT     	= 0,
		NEED_DIG  	= 1,
		DIG	  	= 2,
		NEED_DIG_DEC 	= 3,
		DIG_DEC 	= 4,
		EXP	 	= 5,
		NEED_DIG_EXP 	= 6,
		DIG_EXP 	= 7,
		END 		= 8
	};

	/*
	 * Double pattern
	 * [-|\+]?[0-9]+(\.)?([0-9]+)?([Ee](\+|-)?[0-9]+)?
	 * We could also meet spaces before and/or after
	 * this pattern upon parameters
	 */

	if (space_before) while (isspace(*d)) d++;
	for (st = INIT, p = d ; *p ; p++)
	{

lwnotice("State: %d, *p=%c", st, *p);

		if (isdigit(*p))
		{
			if (st == INIT || st == NEED_DIG) 	st = DIG;
			else if (st == NEED_DIG_DEC) 			st = DIG_DEC;
			else if (st == NEED_DIG_EXP || st == EXP) 	st = DIG_EXP;
			else if (st == DIG || st == DIG_DEC || st == DIG_EXP);
			else lwerror("invalid KML representation");
		}
		else if (*p == '.')
		{
			if      (st == DIG) 				st = NEED_DIG_DEC;
			else    lwerror("invalid KML representation");
		}
		else if (*p == '-' || *p == '+')
		{
			if      (st == INIT) 				st = NEED_DIG;
			else if (st == EXP) 				st = NEED_DIG_EXP;
			else    lwerror("invalid KML representation");
		}
		else if (*p == 'e' || *p == 'E')
		{
			if      (st == DIG || st == DIG_DEC) 		st = EXP;
			else    lwerror("invalid KML representation");
		}
		else if (isspace(*p))
		{
			if (!space_after) lwerror("invalid KML representation");
			if (st == DIG || st == DIG_DEC || st == DIG_EXP)st = END;
			else if (st == NEED_DIG_DEC)			st = END;
			else if (st == END);
			else    lwerror("invalid KML representation");
		}
		else  lwerror("invalid KML representation");
	}

	if (st != DIG && st != NEED_DIG_DEC && st != DIG_DEC && st != DIG_EXP && st != END)
		lwerror("invalid KML representation");

	return atof(d);
}
#endif /* unused */


/**
 * Parse kml:coordinates
 */
static POINTARRAY* parse_kml_coordinates(xmlNodePtr xnode, bool *hasz)
{
	xmlChar *kml_coord;
	bool found;
	POINTARRAY *dpa;
	int seen_kml_dims = 0;
	int kml_dims;
	char *p, *q;
	POINT4D pt;
  double d;

	if (xnode == NULL) lwerror("invalid KML representation");

	for (found = false ; xnode != NULL ; xnode = xnode->next)
	{
		if (xnode->type != XML_ELEMENT_NODE) continue;
		if (!is_kml_namespace(xnode, false)) continue;
		if (strcmp((char *) xnode->name, "coordinates")) continue;

		found = true;
		break;
	}
	if (!found) lwerror("invalid KML representation");

	/* We begin to retrieve coordinates string */
	kml_coord = xmlNodeGetContent(xnode);
	p = (char *) kml_coord;

	/* KML coordinates pattern:     x1,y1 x2,y2
	 *                              x1,y1,z1 x2,y2,z2
	*/

	/* Now we create PointArray from coordinates values */
	/* HasZ, !HasM, 1pt */
	dpa = ptarray_construct_empty(1, 0, 1);

  while (*p && isspace(*p)) ++p;
	for (kml_dims=0; *p ; p++)
	{
//lwnotice("*p:%c, kml_dims:%d", *p, kml_dims);
    if ( isdigit(*p) || *p == '+' || *p == '-' || *p == '.' ) {
			  kml_dims++;
        errno = 0; d = strtod(p, &q);
        if ( errno != 0 ) {
          // TODO: destroy dpa, return NULL
          lwerror("invalid KML representation"); /*: %s", strerror(errno));*/
        }
        if      (kml_dims == 1) pt.x = d;
        else if (kml_dims == 2) pt.y = d;
        else if (kml_dims == 3) pt.z = d;
        else {
          lwerror("invalid KML representation"); /* (more than 3 dimensions)"); */
          // TODO: destroy dpa, return NULL
        }

//lwnotice("after strtod d:%f, *q:%c, kml_dims:%d", d, *q, kml_dims);

        if ( *q && ! isspace(*q) && *q != ',' ) {
          lwerror("invalid KML representation"); /* (invalid character %c follows ordinate value)", *q); */
        }

        /* Look-ahead to see if we're done reading */
        while (*q && isspace(*q)) ++q;
        if ( isdigit(*q) || *q == '+' || *q == '-' || *q == '.' || ! *q ) {
          if ( kml_dims < 2 ) lwerror("invalid KML representation"); /* (not enough ordinates)"); */
          else if ( kml_dims < 3 ) *hasz = false;
          if ( ! seen_kml_dims ) seen_kml_dims = kml_dims;
          else if ( seen_kml_dims != kml_dims ) {
            lwerror("invalid KML representation: mixed coordinates dimension");
          }
          ptarray_append_point(dpa, &pt, LW_TRUE);
          kml_dims = 0;
        }
        p = q-1; /* will be incrementedon next iteration */
//lwnotice("after look-ahead *p:%c, kml_dims:%d", *p, kml_dims);
    } else if ( *p != ',' && ! isspace(*p) ) {
          lwerror("invalid KML representation"); /* (unexpected character %c)", *p); */
    }
	}

	xmlFree(kml_coord);

	/* TODO: we shouldn't need to clone here */
	return ptarray_clone_deep(dpa);
}


/**
 * Parse KML point
 */
static LWGEOM* parse_kml_point(xmlNodePtr xnode, bool *hasz)
{
	POINTARRAY *pa;

	if (xnode->children == NULL) lwerror("invalid KML representation");
	pa = parse_kml_coordinates(xnode->children, hasz);
	if (pa->npoints != 1) lwerror("invalid KML representation");

	return (LWGEOM *) lwpoint_construct(4326, NULL, pa);
}


/**
 * Parse KML lineString
 */
static LWGEOM* parse_kml_line(xmlNodePtr xnode, bool *hasz)
{
	POINTARRAY *pa;

	if (xnode->children == NULL) lwerror("invalid KML representation");
	pa = parse_kml_coordinates(xnode->children, hasz);
	if (pa->npoints < 2) lwerror("invalid KML representation");

	return (LWGEOM *) lwline_construct(4326, NULL, pa);
}


/**
 * Parse KML Polygon
 */
static LWGEOM* parse_kml_polygon(xmlNodePtr xnode, bool *hasz)
{
	int ring;
	xmlNodePtr xa, xb;
	POINTARRAY **ppa = NULL;

	for (xa = xnode->children ; xa != NULL ; xa = xa->next)
	{

		/* Polygon/outerBoundaryIs */
		if (xa->type != XML_ELEMENT_NODE) continue;
		if (!is_kml_namespace(xa, false)) continue;
		if (strcmp((char *) xa->name, "outerBoundaryIs")) continue;

		for (xb = xa->children ; xb != NULL ; xb = xb->next)
		{

			if (xb->type != XML_ELEMENT_NODE) continue;
			if (!is_kml_namespace(xb, false)) continue;
			if (strcmp((char *) xb->name, "LinearRing")) continue;

			ppa = (POINTARRAY**) lwalloc(sizeof(POINTARRAY*));
			ppa[0] = parse_kml_coordinates(xb->children, hasz);

			if (ppa[0]->npoints < 4
			        || (!*hasz && !ptarray_is_closed_2d(ppa[0]))
			        ||  (*hasz && !ptarray_is_closed_3d(ppa[0])))
				lwerror("invalid KML representation");
		}
	}

	for (ring=1, xa = xnode->children ; xa != NULL ; xa = xa->next)
	{

		/* Polygon/innerBoundaryIs */
		if (xa->type != XML_ELEMENT_NODE) continue;
		if (!is_kml_namespace(xa, false)) continue;
		if (strcmp((char *) xa->name, "innerBoundaryIs")) continue;

		for (xb = xa->children ; xb != NULL ; xb = xb->next)
		{

			if (xb->type != XML_ELEMENT_NODE) continue;
			if (!is_kml_namespace(xb, false)) continue;
			if (strcmp((char *) xb->name, "LinearRing")) continue;

			ppa = (POINTARRAY**) lwrealloc((POINTARRAY *) ppa,
			                               sizeof(POINTARRAY*) * (ring + 1));
			ppa[ring] = parse_kml_coordinates(xb->children, hasz);

			if (ppa[ring]->npoints < 4
			        || (!*hasz && !ptarray_is_closed_2d(ppa[ring]))
			        ||  (*hasz && !ptarray_is_closed_3d(ppa[ring])))
				lwerror("invalid KML representation");

			ring++;
		}
	}

	/* Exterior Ring is mandatory */
	if (ppa == NULL || ppa[0] == NULL) lwerror("invalid KML representation");

	return (LWGEOM *) lwpoly_construct(4326, NULL, ring, ppa);
}


/**
 * Parse KML MultiGeometry
 */
static LWGEOM* parse_kml_multi(xmlNodePtr xnode, bool *hasz)
{
	LWGEOM *geom;
	xmlNodePtr xa;

	geom = (LWGEOM *)lwcollection_construct_empty(COLLECTIONTYPE, 4326, 1, 0);

	for (xa = xnode->children ; xa != NULL ; xa = xa->next)
	{

		if (xa->type != XML_ELEMENT_NODE) continue;
		if (!is_kml_namespace(xa, false)) continue;

		if (	   !strcmp((char *) xa->name, "Point")
		        || !strcmp((char *) xa->name, "LineString")
		        || !strcmp((char *) xa->name, "Polygon")
		        || !strcmp((char *) xa->name, "MultiGeometry"))
		{

			if (xa->children == NULL) break;
			geom = (LWGEOM*)lwcollection_add_lwgeom((LWCOLLECTION*)geom, parse_kml(xa, hasz));
		}
	}

	return geom;
}


/**
 * Parse KML
 */
static LWGEOM* parse_kml(xmlNodePtr xnode, bool *hasz)
{
	xmlNodePtr xa = xnode;

	while (xa != NULL && (xa->type != XML_ELEMENT_NODE
	                      || !is_kml_namespace(xa, false))) xa = xa->next;

	if (xa == NULL) lwerror("invalid KML representation");

	if (!strcmp((char *) xa->name, "Point"))
		return parse_kml_point(xa, hasz);

	if (!strcmp((char *) xa->name, "LineString"))
		return parse_kml_line(xa, hasz);

	if (!strcmp((char *) xa->name, "Polygon"))
		return parse_kml_polygon(xa, hasz);

	if (!strcmp((char *) xa->name, "MultiGeometry"))
		return parse_kml_multi(xa, hasz);

	lwerror("invalid KML representation");
	return NULL; /* Never reach */
}
