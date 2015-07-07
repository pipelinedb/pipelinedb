\set ECHO queries
\pset pager off

\i micro-macro.sql

select (std).* from (
    select standardize_address('lex', 'gaz', 'rules', micro, macro) as std
      from addresses) as foo;
