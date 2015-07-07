This directory contains the loader and dumper utility programs.

The loader can convert shape files to an SQL dump suitable for loading into a 
PostGIS enabled PostgreSQL database server. The dumper does the opposite, 
creates a shape file out of an PostGIS table or arbitrary query.

To compile the program from source, simply run "make" in the source directory. 
Then copy the binary into your command search path (or wherever you like). 

For usage information, simply run the programs without any arguments, that 
will display a help screen, and look into the ../doc/man/ directory, 
there are manpages ready for copying into the manual search path on 
unixoid systems.
