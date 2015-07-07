TESTDB=ogc_test_suite
all:
	createdb $(TESTDB) > /dev/null
	createlang plpgsql $(TESTDB)
	psql $(TESTDB) < ../../postgis.sql >/dev/null 2>&1
	psql -a -f 1_schema.sql $(TESTDB) > 1_output.txt 2>&1
	psql -a -f 2_queries.sql $(TESTDB) > 2_output.txt 2>&1
	@echo "---------------------------------------"
	@echo "---* Check results in 2_output.txt *---"
	@echo "---------------------------------------"
	
clean:
	dropdb $(TESTDB)
	rm -f 1_output.txt
	rm -f 2_output.txt
