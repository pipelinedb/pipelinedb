/*-------------------------------------------------------------------------
 * Derived from org.postgresql.PGConnection from jdbc8.0 which is licensed
 * under BSD license.
 *
 * Copyright (c) 2003-2005, PostgreSQL Global Development Group
 * Copyright (c) 2005 Markus Schaber <markus.schaber@logix-tt.com>
 *
 * IDENTIFICATION
 *   $PostgreSQL: pgjdbc/org/postgresql/PGConnection.java,v 1.13 2005/01/17 09:51:40 jurka Exp $
 *
 *-------------------------------------------------------------------------
 */
package org.postgresql;

import java.sql.SQLException;

/**
 * Stub to compile postgis jdbc against
 */
public interface PGConnection {
    public void addDataType(String type, String name);

    public void addDataType(String type, Class klass) throws SQLException;
}

