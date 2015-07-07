-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
----
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.net
--
-- Copyright (C) 2011 Regina Obe <lr@pcorp.us>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- Author: Regina Obe <lr@pcorp.us>
--  
-- This drops extension helper functions
-- and should be called at the end of the extension upgrade file
DROP FUNCTION postgis_extension_remove_objects(text, text);
DROP FUNCTION postgis_extension_drop_if_exists(text, text);
DROP FUNCTION postgis_extension_AddToSearchPath(varchar);
