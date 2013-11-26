--
-- XC_CREATE_FUNCTIONS
--

-- Create a couple of functions used by Postgres-XC tests
-- A function to create table on specified nodes
create or replace function create_table_nodes(tab_schema varchar, nodenums int[], distribution varchar, cmd_suffix varchar)
returns void language plpgsql as $$
declare
	cr_command	varchar;
	nodes		varchar[];
	nodename	varchar;
	nodenames_query varchar;
	nodenames 	varchar;
	node 		int;
	sep			varchar;
	tmp_node	int;
	num_nodes	int;
begin
	nodenames_query := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D'' ORDER BY xc_node_id';
	cr_command := 'CREATE TABLE ' || tab_schema || ' DISTRIBUTE BY ' || distribution || ' TO NODE ';
	for nodename in execute nodenames_query loop
		nodes := array_append(nodes, nodename);
	end loop;
	nodenames := '(';
	sep := '';
	num_nodes := array_length(nodes, 1);
	foreach node in array nodenums loop
		tmp_node := node;
		if (tmp_node < 1 or tmp_node > num_nodes) then
			tmp_node := tmp_node % num_nodes;
			if (tmp_node < 1) then
				tmp_node := num_nodes; 
			end if;
		end if;
		nodenames := nodenames || sep || nodes[tmp_node];
		sep := ', ';
	end loop;
	nodenames := nodenames || ')';
	cr_command := cr_command || nodenames;
	if (cmd_suffix is not null) then
		cr_command := cr_command  || ' ' || cmd_suffix;
	end if;
	execute cr_command;
end;
$$;

-- Add/Delete/change node list of a table
CREATE OR REPLACE FUNCTION alter_table_change_nodes(tab_schema varchar, nodenums int[], command varchar, distribution varchar)
RETURNS BOOLEAN LANGUAGE plpgsql as $$
declare
	cr_command	varchar;
	nodes		varchar[];
	nodename	varchar;
	nodenames_query varchar;
	nodenames	varchar;
	sep		varchar;
	nodenum_new	int[];
	nodenum_res	int[];
	tmp_node	int;
	num_nodes	int;
	node		int;
	check_num	boolean;
	enforce_to	boolean;
BEGIN
	-- Check the command type, only delete/add/to are allowed
	IF command != 'delete' AND command != 'add' AND command != 'to' THEN
		RETURN FALSE;
	END IF;
	nodenames_query := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D'' ORDER BY xc_node_id';
	FOR nodename IN EXECUTE nodenames_query LOOP
		nodes := array_append(nodes, nodename);
	END LOOP;
	nodenames := '(';
	sep := '';
	num_nodes := array_length(nodes, 1);
	enforce_to := FALSE;

	-- Adjust node array according to total number of nodes
	FOREACH node IN ARRAY nodenums LOOP
		tmp_node := node;
		IF (node < 1 OR node > num_nodes) THEN
			-- Enforce the usage of TO here, only safe method
			enforce_to := TRUE;
			tmp_node := node % num_nodes;
			nodenum_new := array_append(nodenum_new, tmp_node);
		END IF;
		nodenum_new := array_append(nodenum_new, tmp_node);
	END LOOP;
	-- Eliminate duplicates
	nodenum_res := array_append(nodenum_res, nodenum_new[1]);
	FOREACH node IN ARRAY nodenum_new LOOP
		check_num := TRUE;
		FOREACH tmp_node IN ARRAY nodenum_res LOOP
			IF (tmp_node = node) THEN
				check_num := FALSE;
			END IF;
		END LOOP;
		-- Fill in result array only if not replicated
		IF check_num THEN
			nodenum_res := array_append(nodenum_res, node);
		END IF;
	END LOOP;

	-- If there is a unique Datanode in cluster, enforce the use of 'TO NODE'
	-- This will avoid any consistency problems
	IF (num_nodes = 1 OR enforce_to) THEN
		command := 'TO';
	END IF;

	-- Finally build query
	cr_command := 'ALTER TABLE ' || tab_schema || ' ' || command || ' NODE ';
	FOREACH node IN ARRAY nodenum_res LOOP
		IF (node > 0 AND node <= num_nodes) THEN
			nodenames := nodenames || sep || nodes[node];
			sep := ', ';
		END IF;
	END LOOP;
	nodenames := nodenames || ')';
	cr_command := cr_command || nodenames;

	-- Add distribution if necessary
	IF (distribution IS NOT NULL) then
		cr_command := cr_command  || ', DISTRIBUTE BY ' || distribution;
	END IF;

	-- Launch it
	EXECUTE cr_command;
	RETURN TRUE;
END;
$$;

-- A function to return data node name given a node number
CREATE OR REPLACE FUNCTION get_xc_node_name(node_num int) RETURNS varchar LANGUAGE plpgsql AS $$
DECLARE
	r		pgxc_node%rowtype;
	node		int;
	nodenames_query	varchar;
BEGIN
	nodenames_query := 'SELECT * FROM pgxc_node  WHERE node_type = ''D'' ORDER BY xc_node_id';

	node := 1;
	FOR r IN EXECUTE nodenames_query LOOP
		IF node = node_num THEN
			RETURN r.node_name;
		END IF;
		node := node + 1;
	END LOOP;
	RETURN 'NODE_?';
END;
$$;

-- A function to check whether a certain transaction was prepared on a specific data node given its number
CREATE OR REPLACE FUNCTION is_prepared_on_node(txn_id varchar, nodenum int) RETURNS bool LANGUAGE plpgsql AS $$
DECLARE
	nodename	varchar;
	qry		varchar;
	r		pg_prepared_xacts%rowtype;
BEGIN
	nodename := (SELECT get_xc_node_name(nodenum));
	qry := 'EXECUTE DIRECT ON (' || nodename || ') ' || chr(39) || 'SELECT * FROM pg_prepared_xacts' || chr(39);

	FOR r IN EXECUTE qry LOOP
		IF r.gid = txn_id THEN
			RETURN true;
		END IF;
	END LOOP;
	RETURN false;
END;
$$;

-- A function to execute direct a query on a data node specified by the node number.
create or replace function exec_util_on_node(query varchar, nodenum int) returns void as
$D$
DECLARE
	str varchar;
	node_name varchar;
BEGIN
	node_name = get_xc_node_name(nodenum);
	str = 'execute direct on (' || node_name || ') $$ ' || query || ' $$'  ;
	execute str;
END $D$ language plpgsql;

-- A function to return a generic data node name given xc_node_id (called node_id in catalog)
CREATE OR REPLACE FUNCTION get_xc_node_name_gen(node_id int) RETURNS varchar LANGUAGE plpgsql AS $$
DECLARE
	r		pgxc_node%rowtype;
	nodenames_query	varchar;
	node		int;
BEGIN
	nodenames_query := 'SELECT * FROM pgxc_node  WHERE node_type = ''D'' ORDER BY xc_node_id';

	node := 1;
	FOR r IN EXECUTE nodenames_query LOOP
		IF r.node_id = node_id THEN
			RETURN 'NODE_' || node;
		END IF;
		node := node + 1;
	END LOOP;
	RETURN 'NODE_?';
END;
$$;
