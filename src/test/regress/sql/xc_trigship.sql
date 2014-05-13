create schema xc_trigship;
set search_path to xc_trigship,public;

\set EXP 'EXPLAIN  (verbose, num_nodes off, nodes off, costs off)'
\set CHECK_LOG 'select trigdata, node_type from xc_auditlog, pgxc_node where lower(nodename) = lower(node_name) order by trigdata collate "POSIX"'
\set CHECK_TABLE 'select * from xctrigemp1 order by id'
\set RUN_UPDATE 'update xctrigemp1 set empsal = empsal + 1000 where empname != $$Dummy$$';
\set RUN_DELETE 'delete from xctrigemp1 where empname || now() = $$Dummy$$ || now()';
\set TRUNC 'truncate table xc_auditlog, xctrigemp1';
\set RUN_INSERT 'insert into xctrigemp1 values (1, $$Akash$$, 1000, $$AkashAddress$$), (2, $$Prakash$$, 1500, $$PrakashAddress$$), (3, $$Vikas$$, NULL, $$VikasAddress$$), (4, $$Samar$$, -9999, $$SamarAddress$$), (5, $$Sanika$$, 2000, $$SanikaAddress$$), (-1, $$Dummy$$, 9999, $$DummyAddress$$)'

-- Create Objects
-----------------

-- Base table on which the DML operations would be fired.
create table xctrigemp1 (id int unique, empname varchar unique, empsal float, empadd varchar, trigop varchar) distribute by replication;

-- Table where all the logs of DML operations would be dumped.
create table xc_auditlog (trigdata varchar, nodename varchar) distribute by replication;


-- Convenience function to be used by all the trigger functions to dump all the trigger
-- information including name, operation etc into the xc_auditlog table.
CREATE FUNCTION xc_auditlog_func (tg_name name, tg_op text, tg_level text, tg_when text,
	                         oldrow text, newrow text, nodename name) returns void as $$
DECLARE
	trigdata varchar;
BEGIN
	trigdata := left(tg_level, 1) || left(tg_op, 1) || left(tg_when, 1) || ' ' || tg_name;
	if (tg_level = 'ROW') then
		if (oldrow is not null) then
			trigdata := trigdata || ' ' || oldrow;
		end if;
		if (newrow is not null) then
			trigdata := trigdata || ' ' || newrow;
		end if;
	end if;
	insert into xc_auditlog values (trigdata, nodename::varchar);
END
$$ language plpgsql;



-- STATEMENT trigger function. This also dumps everything except the row.
CREATE FUNCTION xc_stmttrig_func() RETURNS TRIGGER AS $$
BEGIN
	perform xc_auditlog_func(TG_NAME, TG_OP, TG_LEVEL, TG_WHEN, NULL, NULL, pgxc_node_str());
	RETURN NULL;
END $$ immutable LANGUAGE 'plpgsql';


-- A few statement triggers.
CREATE TRIGGER xc_stmttrig1 BEFORE INSERT OR UPDATE OR DELETE ON xctrigemp1
	FOR EACH STATEMENT EXECUTE PROCEDURE xc_stmttrig_func();
CREATE TRIGGER xc_stmttrig2 AFTER INSERT OR UPDATE OR DELETE ON xctrigemp1
	FOR EACH STATEMENT EXECUTE PROCEDURE xc_stmttrig_func();


-- Main trigger function to be used by triggers. It skips the DML operation if
-- salary is invalid. And it inserts a 'default' salary amount if it is NULL.

create function xc_brt_func() returns trigger as
$$
begin
	IF (TG_OP = 'DELETE') then
		OLD.trigop = OLD.trigop || '_' || left(TG_OP, 1) || left(TG_WHEN, 1) || '_' || TG_NAME;
		perform xc_auditlog_func(TG_NAME, TG_OP, TG_LEVEL, TG_WHEN, OLD::text, NULL, pgxc_node_str());
		return OLD;

	ELSE
		if NEW.trigop is NULL then
			NEW.trigop = '';
		end if;

		IF (TG_OP = 'INSERT') then
			IF NEW.empsal IS NULL THEN
				NEW.empsal := 9999;
			END IF;
		END IF;

		NEW.trigop = NEW.trigop || '_' || left(TG_OP, 1) || left(TG_WHEN, 1) || '_' || TG_NAME;
		perform xc_auditlog_func(TG_NAME, TG_OP, TG_LEVEL, TG_WHEN, NULL, NEW::text, pgxc_node_str());

	END IF;

	IF NEW.empsal < 0 THEN
		return NULL;
	ELSE
		return NEW;
	END IF;

end
$$ immutable language plpgsql;


-- Additional trigger function so that we can have a mix of shippable and non-shippable
-- triggers. Turn on or off the volatility of this function as required.
create function xc_brt_vol_func() returns trigger as
$$
begin
	IF (TG_OP = 'DELETE') then
		OLD.trigop = OLD.trigop || '_' || left(TG_OP, 1) || left(TG_WHEN, 1) || '_' || TG_NAME;
		perform xc_auditlog_func(TG_NAME, TG_OP, TG_LEVEL, TG_WHEN, OLD::text, NULL, pgxc_node_str());
		return OLD;
	ELSE
		NEW.trigop = NEW.trigop || '_' || left(TG_OP, 1) || left(TG_WHEN, 1) || '_' || TG_NAME;
		perform xc_auditlog_func(TG_NAME, TG_OP, TG_LEVEL, TG_WHEN, NULL, NEW::text, pgxc_node_str());
	END IF;
	return NEW;
end
$$ immutable language plpgsql; -- Declare this initially immutable, to be changed later.


-- A few BEFORE ROW triggers to start with
CREATE TRIGGER brtrig2 BEFORE INSERT ON xctrigemp1
	FOR EACH ROW EXECUTE PROCEDURE xc_brt_vol_func();
CREATE TRIGGER brtrig3 BEFORE INSERT OR UPDATE OR DELETE ON xctrigemp1
	FOR EACH ROW EXECUTE PROCEDURE xc_brt_func();
CREATE TRIGGER brtrig1 BEFORE INSERT OR UPDATE OR DELETE ON xctrigemp1
	FOR EACH ROW EXECUTE PROCEDURE xc_brt_func();

\set QUIET false

-- Setup is now ready to test shippability with BR triggers.

-- All functions are immutable, so all the queries should be shippable or should
-- invoke all of the row triggers on datanode if the query is not shippable.
-- Statement triggers are always executed on coordinator.


:RUN_INSERT;
:EXP :RUN_UPDATE;
:RUN_UPDATE;
:RUN_DELETE;
:CHECK_LOG;
:CHECK_TABLE;

-- Make the statement trigger non-shippable. Even with this, the statement
-- should be shippable. Statement triggers are anyway invoked separately for
-- FQS query.
alter function xc_stmttrig_func() volatile;
:TRUNC;
:RUN_INSERT;
:EXP :RUN_UPDATE;
:RUN_UPDATE;
:RUN_DELETE;
:CHECK_LOG;
:CHECK_TABLE;

-- Make trigger brtrig2 non-shippable.
-- Now, the INSERTs should execute *all* of the ROW triggers on coordinator.
-- Also, the brtrig2 trigger being non-shippable should not affect the
-- shippability of DELETEs or UPDATEs. They should still invoke triggers on datanodes.
-- This is because brtrig2 is defined only for INSERT.
alter function xc_stmttrig_func() immutable; -- (Turn stmt trigger back to shippable)
alter function xc_brt_vol_func() volatile;
:TRUNC;
:RUN_INSERT;
:RUN_UPDATE;
:RUN_DELETE;
:CHECK_LOG;
:CHECK_TABLE;

-- Now include UPDATE in the brtrig2 trigger definition, and verify
-- that the UPDATE is now invoking triggers on coordinator. DELETEs should
-- continue running on datanodes.
DROP TRIGGER brtrig2 on xctrigemp1;
CREATE TRIGGER brtrig2 BEFORE INSERT OR UPDATE ON xctrigemp1
	FOR EACH ROW EXECUTE PROCEDURE xc_brt_vol_func();
:TRUNC;
:RUN_INSERT;
:RUN_UPDATE;
:RUN_DELETE;
:CHECK_LOG;
:CHECK_TABLE;


-- Keep 2 shippable BR triggers, and 1 non-shippable AR trigger.
-- Since there are non-shippable AR triggers, the BR triggers also should be
-- executed on coordinator even when they are all shippable.
DROP TRIGGER brtrig1 on xctrigemp1;
DROP TRIGGER brtrig2 on xctrigemp1;
DROP TRIGGER brtrig3 on xctrigemp1;
CREATE TRIGGER brtrig3 BEFORE INSERT OR UPDATE OR DELETE ON xctrigemp1
	FOR EACH ROW EXECUTE PROCEDURE xc_brt_func();
CREATE TRIGGER brtrig1 BEFORE INSERT OR UPDATE OR DELETE ON xctrigemp1
	FOR EACH ROW EXECUTE PROCEDURE xc_brt_func();
CREATE TRIGGER artrig AFTER INSERT OR UPDATE OR DELETE ON xctrigemp1
	FOR EACH ROW EXECUTE PROCEDURE xc_brt_vol_func();
alter function xc_brt_func() immutable;
alter function xc_brt_vol_func() volatile;
:TRUNC;
:RUN_INSERT;
:RUN_INSERT; -- This should invoke the constraint triggers.
:RUN_UPDATE;
update xctrigemp1 set empname = 'Akash' where empname != $$Dummy$$; -- Constraint triggers
:RUN_DELETE;
:CHECK_LOG;
:CHECK_TABLE;


-- Make the above AR trigger shippable. This will cause both AR and BR triggers
-- to run on datanode.
alter function xc_brt_vol_func() immutable;
:TRUNC;
:RUN_INSERT;
:RUN_UPDATE;
:RUN_DELETE;
:CHECK_LOG;
:CHECK_TABLE;


-- Make one of the BR triggers non-shippable. This should not affect AR triggers.
-- They should continue to run on datanode.
alter function xc_brt_func() volatile;
:TRUNC;
:RUN_INSERT;
:RUN_UPDATE;
:RUN_DELETE;
:CHECK_LOG;
:CHECK_TABLE;


-- Drop all BR triggers. We have only AR triggers and they are shippable. So the query should be FQSed.
DROP TRIGGER brtrig1 on xctrigemp1;
DROP TRIGGER brtrig3 on xctrigemp1;
:TRUNC;
:RUN_INSERT;
:EXP :RUN_UPDATE;
:RUN_UPDATE;
:RUN_DELETE;
:CHECK_LOG;
:CHECK_TABLE;

-- When non-shippable AR triggers are present, we do execute BR triggers also on coordinator,
-- and so in the remote UPDATE stmt, we update *all* the columns. But when we do
-- not have BR triggers in the first place, this should revert back the remote
-- UPDATE statement to the normal behaviour where it updates only the user-supplied
-- columns.
-- Make the AR trigger function non-shippable.
alter function xc_brt_vol_func() volatile;
:TRUNC;
:RUN_INSERT;
:EXP :RUN_UPDATE;
:RUN_UPDATE;
:RUN_DELETE;
:CHECK_LOG;
:CHECK_TABLE;

drop schema xc_trigship cascade;
