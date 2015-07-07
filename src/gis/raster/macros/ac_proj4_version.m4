dnl **********************************************************************
dnl *
dnl * PostGIS - Spatial Types for PostgreSQL
dnl * http://postgis.net
dnl * Copyright 2008 Mark Cave-Ayland
dnl *
dnl * This program is free software; you can redistribute it and/or
dnl * modify it under the terms of the GNU General Public License
dnl * as published by the Free Software Foundation; either version 2
dnl * of the License, or (at your option) any later version.
dnl *
dnl * This program is distributed in the hope that it will be useful,
dnl * but WITHOUT ANY WARRANTY; without even the implied warranty of
dnl * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
dnl * GNU General Public License for more details.
dnl *
dnl * You should have received a copy of the GNU General Public License
dnl * along with this program; if not, write to the Free Software Foundation,
dnl * Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
dnl *
dnl **********************************************************************

dnl
dnl Return the PROJ.4 version number
dnl

AC_DEFUN([AC_PROJ_VERSION], [
	AC_RUN_IFELSE(
        	[AC_LANG_PROGRAM([
		#ifdef HAVE_STDINT_H
        		#include <stdio.h>
		#endif
		#include "proj_api.h"
	], 
	[
		FILE *fp; 

		fp = fopen("conftest.out", "w"); 
		fprintf(fp, "%d\n", PJ_VERSION); 
		fclose(fp)])
	],
        [
		dnl The program ran successfully, so return the version number in the form MAJORMINOR
		$1=`cat conftest.out | sed 's/\([[0-9]]\)\([[0-9]]\)\([[0-9]]\)/\1\2/'`
	],
        [
		dnl The program failed so return an empty variable
		$1=""
	]
        )
])

