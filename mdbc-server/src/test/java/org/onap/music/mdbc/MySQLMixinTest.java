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

package org.onap.music.mdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.onap.music.mdbc.mixins.MySQLMixin;

import ch.vorburger.mariadb4j.DB;

public class MySQLMixinTest {

	public static final String DATABASE = "mdbcTest";
	public static final String TABLE= "Persons";
	public static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS " + TABLE + " (\n" +
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
		//start embedded mariadb
		DB db = DB.newEmbeddedDB(13306);
		db.start();
		db.createDB(DATABASE);
	}
	
	@AfterClass
	public static void close() throws Exception {
		
	}
	
	@Before
	public void beforeTest() throws SQLException {
		this.conn = DriverManager.getConnection("jdbc:mariadb://localhost:13306/"+DATABASE, "root", "");
		this.mysqlMixin = new MySQLMixin(null, "localhost:13306/"+DATABASE, conn, null);
	}
	
	@Test
	public void testGetDataBaseName() throws SQLException {
		assertEquals(DATABASE, mysqlMixin.getDatabaseName());
	}

}
