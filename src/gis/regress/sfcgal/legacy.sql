--
-- These tests serve the purpose of ensuring compatibility with 
-- old versions of postgis users.
--
-- Their use rely on loading the legacy.sql script.
-- This file also serves as a testcase for uninstall_legacy.sql
--

SET postgis.backend = 'sfcgal';

\cd :regdir
\i legacy.sql
