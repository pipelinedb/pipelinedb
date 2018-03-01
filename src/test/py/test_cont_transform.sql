--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.3
-- Dumped by pg_dump version 9.5.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pipeline; Type: COMMENT; Schema: -; Owner: derek
--

COMMENT ON DATABASE pipeline IS 'default administrative connection database';


--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

--
-- Name: test_tg(); Type: FUNCTION; Schema: public; Owner: derek
--

CREATE FUNCTION test_tg() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  BEGIN
   INSERT INTO test_t (x, y) VALUES (NEW.x, NEW.y);
   RETURN NEW;
  END;
  $$;


ALTER FUNCTION public.test_tg() OWNER TO derek;

SET default_tablespace = '';

--
-- Name: ct_stream; Type: FOREIGN TABLE; Schema: public; Owner: derek
--

CREATE FOREIGN TABLE ct_stream (
    x integer,
    y text,
    arrival_timestamp timestamp with time zone
)
SERVER pipeline_streams;


ALTER FOREIGN TABLE ct_stream OWNER TO derek;

--
-- Name: cv_stream; Type: FOREIGN TABLE; Schema: public; Owner: derek
--

CREATE FOREIGN TABLE cv_stream (
    x integer,
    y text,
    arrival_timestamp timestamp with time zone
)
SERVER pipeline_streams;


ALTER FOREIGN TABLE cv_stream OWNER TO derek;

--
-- Name: test_ct1; Type: CONTINUOUS TRANSFORM; Schema: public; Owner: derek
--

CREATE CONTINUOUS TRANSFORM public.test_ct1 AS  SELECT x,
    y
   FROM ct_stream
  WHERE (mod(x, 2) = 0) THEN EXECUTE PROCEDURE pipeline_stream_insert('cv_stream', 'cv_stream');



ALTER TABLE test_ct1 OWNER TO derek;

--
-- Name: test_ct2; Type: CONTINUOUS TRANSFORM; Schema: public; Owner: derek
--

CREATE CONTINUOUS TRANSFORM public.test_ct2 AS  SELECT x,
    y
   FROM ct_stream THEN EXECUTE PROCEDURE test_tg();



ALTER TABLE test_ct2 OWNER TO derek;

--
-- Name: test_cv; Type: CONTINUOUS VIEW; Schema: public; Owner: derek
--

CREATE CONTINUOUS VIEW public.test_cv AS  SELECT count(*) AS count
   FROM cv_stream;

SELECT pg_catalog.setval('test_cv_seq', 1, true);

SET continuous_query_materialization_table_updatable = on;



ALTER TABLE test_cv OWNER TO derek;

SET default_with_oids = false;

--
-- Name: test_t; Type: TABLE; Schema: public; Owner: derek
--

CREATE TABLE test_t (
    y text,
    x integer
);


ALTER TABLE test_t OWNER TO derek;

--
-- Data for Name: test_cv_mrel; Type: TABLE DATA; Schema: public; Owner: derek
--

COPY test_cv_mrel (count, "$pk") FROM stdin;
2	1
\.


--
-- Data for Name: test_t; Type: TABLE DATA; Schema: public; Owner: derek
--

COPY test_t (y, x) FROM stdin;
hello	1
world	2
\.


--
-- Name: public; Type: ACL; Schema: -; Owner: derek
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM derek;
GRANT ALL ON SCHEMA public TO derek;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

