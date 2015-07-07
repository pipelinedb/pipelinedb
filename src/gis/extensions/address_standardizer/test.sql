\set ECHO queries
\pset pager off

select * from parse_address('123 Main Street, Kansas City, MO 45678');

\i /usr/share/postgresql/9.2/extension/us-lex.sql
\i /usr/share/postgresql/9.2/extension/us-gaz.sql
\i /usr/share/postgresql/9.2/extension/us-rules.sql

select * from standardize_address('lex'::text, 'gaz'::text, 'rules'::text, '123 Main Street'::text, 'Kansas City, MO 45678'::text);

\q
