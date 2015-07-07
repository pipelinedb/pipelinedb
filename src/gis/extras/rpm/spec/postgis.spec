%{!?javabuild:%define	javabuild 1}
%{!?utils:%define	utils 1}
%{!?gcj_support:%define	gcj_support 1}

Summary:	Geographic Information Systems Extensions to PostgreSQL
Name:		postgis
Version:	1.2.0
Release:	2%{?dist}
License:	GPL
Group:		Applications/Databases
Source0:	http://download.osgeo.org/postgis/source/%{name}-%{version}.tar.gz
Source4:	filter-requires-perl-Pg.sh
Patch1:		postgis-configure.patch
Patch2:		postgis-javamakefile.patch
URL:		http://postgis.net/
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	postgresql-devel, proj-devel, geos-devel, byacc, proj-devel, flex, postgresql-jdbc
Requires:	postgresql, geos, proj

%description
PostGIS adds support for geographic objects to the PostgreSQL object-relational
database. In effect, PostGIS "spatially enables" the PostgreSQL server,
allowing it to be used as a backend spatial database for geographic information
systems (GIS), much like ESRI's SDE or Oracle's Spatial extension. PostGIS 
follows the OpenGIS "Simple Features Specification for SQL" and has been 
certified as compliant with the "Types and Functions" profile.

%if %javabuild
%package jdbc
Summary:	The JDBC driver for PostGIS
Group:		Applications/Databases
License:	LGPL
Requires:	postgis
BuildRequires:  ant >= 0:1.6.2, junit >= 0:3.7

%if %{gcj_support}
BuildRequires:		gcc-java
Requires(post):		java-1.4.2-gcj-compat
Requires(postun):	java-1.4.2-gcj-compat
%endif

%description jdbc
The postgis-jdbc package provides the essential jdbc driver for PostGIS.
%endif

%if %utils
%package utils
Summary:	The utils for PostGIS
Group:		Applications/Databases
Requires:	postgis, perl-DBD-Pg

%description utils
The postgis-utils package provides the utilities for PostGIS.
%endif

%define __perl_requires %{SOURCE4}

%prep
%setup -q
%patch1 -p0
%patch2 -p0

%build
%configure 
make %{?_smp_mflags} LPATH=`pg_config --pkglibdir` shlib="%{name}.so"

%if %javabuild
export MAKEFILE_DIR=%{_builddir}/%{name}-%{version}/java/jdbc
JDBC_VERSION_RPM=`rpm -ql postgresql-jdbc| grep 'jdbc.jar$'|awk -F '/' '{print $5}'`
sed 's/postgresql.jar/'${JDBC_VERSION_RPM}'/g' $MAKEFILE_DIR/Makefile > $MAKEFILE_DIR/Makefile.new
mv -f $MAKEFILE_DIR/Makefile.new $MAKEFILE_DIR/Makefile
make -C java/jdbc
%endif

%if %utils
 make -C utils
%endif

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}
install -d %{buildroot}%{_libdir}/pgsql/
install lwgeom/liblwgeom.so* %{buildroot}%{_libdir}/pgsql/
install -d  %{buildroot}%{_datadir}/pgsql/contrib/
install -m 644 *.sql %{buildroot}%{_datadir}/pgsql/contrib/
rm -f  %{buildroot}%{_libdir}/liblwgeom.so*
rm -f  %{buildroot}%{_datadir}/*.sql

%if %javabuild
install -d %{buildroot}%{_javadir}
install -m 755 java/jdbc/%{name}_%{version}.jar %{buildroot}%{_javadir}
%if %{gcj_support}
aot-compile-rpm
%endif
%endif

strip %{buildroot}/%{_libdir}/gcj/%{name}/*.jar.so

%if %utils
install -d %{buildroot}%{_datadir}/%{name}
install -m 644 utils/*.pl %{buildroot}%{_datadir}/%{name}
%endif

%clean
rm -rf %{buildroot}

%post -p %{_bindir}/rebuild-gcj-db

%postun -p %{_bindir}/rebuild-gcj-db

%files
%defattr(-,root,root)
%doc COPYING CREDITS NEWS TODO README.%{name} TODO doc/html loader/README.* doc/%{name}.xml  doc/ZMSgeoms.txt 
%attr(755,root,root) %{_bindir}/*
%attr(755,root,root) %{_libdir}/pgsql/liblwgeom.so*
%{_datadir}/pgsql/contrib/*.sql

%if %javabuild
%files jdbc
%defattr(-,root,root)
%doc java/jdbc/COPYING_LGPL java/jdbc/README
%attr(755,root,root) %{_javadir}/%{name}_%{version}.jar
%if %{gcj_support}
%dir %{_libdir}/gcj/%{name}
%{_libdir}/gcj/%{name}/*.jar.so
%{_libdir}/gcj/%{name}/*.jar.db
%endif
%endif

%if %utils
%files utils
%defattr(-,root,root)
%doc utils/README
%attr(755,root,root) %{_datadir}/%{name}/test_estimation.pl
%attr(755,root,root) %{_datadir}/%{name}/profile_intersects.pl
%attr(755,root,root) %{_datadir}/%{name}/test_joinestimation.pl
%attr(644,root,root) %{_datadir}/%{name}/create_undef.pl
%attr(644,root,root) %{_datadir}/%{name}/%{name}_proc_upgrade.pl
%attr(644,root,root) %{_datadir}/%{name}/%{name}_restore.pl
%endif

%changelog
* Mon Dec 26 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 1.2.0-2
- More spec file fixes per bugzilla review #220743

* Mon Dec 25 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 1.2.0-1
- Initial submission for Fedora Core Extras
- Spec file changes and fixes per FC Extras packaging guidelines

* Fri Jun 23 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 1.1.2-2
- Update to 1.1.2

* Tue Dec 22 2005 - Devrim GUNDUZ <devrim@commandprompt.com> 1.1.0-2
- Final fixes for 1.1.0

* Tue Dec 06 2005 - Devrim GUNDUZ <devrim@gunduz.org>
- Update to 1.1.0

* Mon Oct 03 2005 - Devrim GUNDUZ <devrim@gunduz.org>
- Make PostGIS build against pgxs so that we don't need PostgreSQL sources.
- Fixed all build errors except jdbc (so, defaulted to 0)
- Added new files under %%utils
- Removed postgis-jdbc2-makefile.patch (applied to -head)
                                                                                                    
* Tue Sep 27 2005 - Devrim GUNDUZ <devrim@gunduz.org>
- Update to 1.0.4

* Sun Apr 20 2005 - Devrim GUNDUZ <devrim@gunduz.org>
- 1.0.0 Gold

* Sun Apr 17 2005 - Devrim GUNDUZ <devrim@gunduz.org>
- Modified the spec file so that we can build JDBC2 RPMs...
- Added -utils RPM to package list.

* Fri Apr 15 2005 - Devrim GUNDUZ <devrim@gunduz.org>
- Added preun and postun scripts.

* Sat Apr 09 2005 - Devrim GUNDUZ <devrim@gunduz.org>
- Initial RPM build
- Fixed libdir so that PostgreSQL installations will not complain about it.
- Enabled --with-geos and modified the old spec.
