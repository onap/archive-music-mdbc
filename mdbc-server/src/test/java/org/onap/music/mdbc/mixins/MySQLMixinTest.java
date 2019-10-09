/*
 * ============LICENSE_START====================================================
 * org.onap.music.mdbc
 * =============================================================================
 * Copyright (C) 2018 AT&T Intellectual Property. All rights reserved.
 * =============================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END======================================================
 */

package org.onap.music.mdbc.mixins;

import java.util.Properties;
import org.junit.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.onap.music.mdbc.MdbcTestUtils;
import org.onap.music.mdbc.MdbcTestUtils.DBType;
import org.onap.music.mdbc.mixins.MySQLMixin;

import ch.vorburger.mariadb4j.DB;

public class MySQLMixinTest {

	public static final String DATABASE = "mdbctest";
	public static final String TABLE= MdbcTestUtils.getMariaDBDBName();
	public static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS " + MdbcTestUtils.getMariaDBDBName()+ " (\n" +
            "    PersonID int,\n" +
            "    LastName varchar(255),\n" +
            "    FirstName varchar(255),\n" +
            "    Address varchar(255),\n" +
            "    City varchar(255),\n" +
            "    PRIMARY KEY (PersonID,LastName)" +
            ");";
	
	
	Connection conn;
	MySQLMixin mysqlMixin;
	
	
	
	@BeforeClass
	public static void init() throws Exception {
		Class.forName("org.mariadb.jdbc.Driver");
		MdbcTestUtils.startMariaDb();

	}
	
	@AfterClass
	public static void close() throws Exception {
		MdbcTestUtils.cleanDatabase(DBType.MySQL);
	}
	
	@Before
	public void beforeTest() throws SQLException {
		this.conn = MdbcTestUtils.getConnection(DBType.MySQL);
		Properties info = new Properties();
		this.mysqlMixin = new MySQLMixin(null, null, conn, info);
	}
	
	@Test
	public void testGetDataBaseName() throws SQLException {
		Assert.assertEquals(MdbcTestUtils.getMariaDBDBName().toUpperCase(), mysqlMixin.getDatabaseName());
	}

}
