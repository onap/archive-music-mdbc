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
package org.onap.music.mdbc.tests;

//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.HashSet;
//import java.util.Properties;
//import java.util.Set;
//
//import org.h2.tools.Server;
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;


//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
//@RunWith(ConcurrentTestRunner.class)
public class ConnectionTest {
//
////	static {
////		System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
////		System.setProperty(org.slf4j.impl.SimpleLogger.LOG_FILE_KEY, String.format("ComparativeAnalysisTest-%d.log", System.currentTimeMillis()));
////	}
//	private static final Logger LOG = LoggerFactory.getLogger(ConnectionTest.class);
//
//	Set<Thread> runningThreads = new HashSet<Thread>();
//
//	@BeforeClass
//	public static void setUpBeforeClass() throws Exception {
//
//	}
//
//	@AfterClass
//	public static void tearDownAfterClass() throws Exception {
//		
//	}
//
//	@Before
//	public void setUp() throws Exception {
//
//	}
//
//	@After
//	public void tearDown() throws Exception {
//
//	}
//
//	//@Test
//	public void test01() {
//		System.out.println("TEST 1: Getting ready for testing connection to Cassandra");
//
//		final CassandraConnector client = new CassandraConnector();
//		final String ipAddress = "localhost";
//		final int port = 9042;
//		LOG.info("Connecting to IP Address " + ipAddress + ":" + port + "...");
//		client.connect(ipAddress, port);
//		client.close();
//		System.out.println();
//	}
//	
//	/**
//	 * Tests for using jdbc as well as mdbc. In order to use, must have mysql and
//	 * running locally. Must have a database EMP created in the
//	 * mysql db. Uses "Driver.getConnection(com.mysql.jdbc.Driver)" for jdbc connection
//	 * 
//	 */
//	//@Test
//	public void test02() {
//		System.out.println("TEST 2: Getting ready for testing connection via jdbc");
//		// JDBC driver name and database URL
//		final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
//		final String DB_URL = "jdbc:mysql://localhost/EMP";
//
//		//  Database credentials
//		final String USER = "alice";
//		final String PASS = "bob";
//		Properties connectionProps = new Properties();
//	    connectionProps.put("user", USER);
//	    connectionProps.put("password", PASS);
//
//	    System.out.println("Connecting directly to database...");
//		connectViaDriverManager(JDBC_DRIVER, DB_URL, connectionProps);
//		System.out.println();
//	}
//
//	/**
//	 * Performs same test as @test02() except this test uses mdbc.
//	 * 
//	 * In order to use, must have mysql and Cassandra services running locally. Must 
//	 * have a database EMP created in the mysql db. Uses 
//	 * "Driver.getConnection(org.onap.music.mdbc.ProxyDriver)" for mdbc
//	 * connection
//	 */
//	//@Test
//	public void test03() {
//		System.out.println("TEST 3: Getting ready for testing connection via mdbc");
//		//  Database credentials
//		final String USER = "alice";
//		final String PASS = "bob";
//		Properties connectionProps = new Properties();
//		connectionProps.put("user", USER);
//		connectionProps.put("password", PASS);
//	
//		final String MDBC_DRIVER = "org.onap.music.mdbc.ProxyDriver";
//		final String MDBC_DB_URL = "jdbc:mdbc://localhost/TEST";
//		final String MDBC_DB_MIXIN = "mysql";
//		connectionProps.put("MDBC_DB_MIXIN", MDBC_DB_MIXIN);
//
//		System.out.println("Connecting to database via mdbc");
//		connectViaDriverManager(MDBC_DRIVER, MDBC_DB_URL, connectionProps);
// 	    System.out.println();
//	}
//
//	/**
//	 * Performs same test as @test02() except this test uses mdbc.
//	 * 
//	 * In order to use, must have mysql and Cassandra services running locally. Must 
//	 * have a database EMP created in the mysql db. Uses 
//	 * "Driver.getConnection(org.onap.music.mdbc.ProxyDriver)" for mdbc
//	 * connection
//	 * 
//	 * Uses preparedStatements
//	 */
//	//@Test
//	public void test03point5() {
//		System.out.println("TEST 3.5: Getting ready for testing connection via mdbc w/ PreparedStatement");
//		//  Database credentials
//		final String USER = "alice";
//		final String PASS = "bob";
//		Properties connectionProps = new Properties();
//		connectionProps.put("user", USER);
//		connectionProps.put("password", PASS);
//	
//		final String MDBC_DRIVER = "org.onap.music.mdbc.ProxyDriver";
//		final String MDBC_DB_URL = "jdbc:mdbc://localhost/EMP";
//		//final String MDBC_DRIVER = "org.h2.Driver";
//		//final String MDBC_DB_URL = "jdbc:h2:tcp://localhost:9092/~/test";
//		final String MDBC_DB_MIXIN = "mysql";
//		connectionProps.put("MDBC_DB_MIXIN", MDBC_DB_MIXIN);
//		
//		System.out.println("Connecting to database via mdbc");
//		Connection conn = null;
//		PreparedStatement stmt = null;
//		try {
//			//STEP 2: Register JDBC driver
//			Class.forName(MDBC_DRIVER);
//
//			//STEP 3: Open a connection
//			conn = DriverManager.getConnection(MDBC_DB_URL, connectionProps);
//			conn.setAutoCommit(false);
//
//			//STEP 4: Execute a query
//			System.out.println("Inserting into DB");
//			stmt = conn.prepareStatement("INSERT INTO EMPLOYEE (id, first, last, age) VALUES (?, ?, ?, ?)");
//			stmt.setString(1, null);
//			stmt.setString(2, "John");
//			stmt.setString(3, "Smith");
//			stmt.setInt(4, 20);
//			stmt.execute();
//			
//			System.out.println("Inserting again into DB");
//			stmt.setString(2, "Jane");
//			stmt.setInt(4, 30);
//			stmt.execute();
//			
//			stmt.close();
//			
//			conn.commit();
//			
//			System.out.println("Querying the DB");
//			stmt = conn.prepareStatement("SELECT id, first, last, age FROM EMPLOYEE WHERE age < ?");
//			stmt.setInt(1, 25);
//			ResultSet rs = stmt.executeQuery();
//			//STEP 5: Extract data from result set
//			while(rs.next()) {
//				//Retrieve by column name
//				int id  = rs.getInt("id");
//				int age = rs.getInt("age");
//				String first = rs.getString("first");
//				String last = rs.getString("last");
//
//				//Display values
//				//*
//				System.out.print("ID: " + id);
//				System.out.print(", Age: " + age);
//				System.out.print(", First: " + first);
//				System.out.println(", Last: " + last);
//				//*/
//			}
//			
//			System.out.println("Querying again");
//			stmt.setInt(1, 35);
//			rs = stmt.executeQuery();
//			//STEP 5: Extract data from result set
//			while(rs.next()) {
//				//Retrieve by column name
//				int id  = rs.getInt("id");
//				int age = rs.getInt("age");
//				String first = rs.getString("first");
//				String last = rs.getString("last");
//
//				//Display values
//				//*
//				System.out.print("ID: " + id);
//				System.out.print(", Age: " + age);
//				System.out.print(", First: " + first);
//				System.out.println(", Last: " + last);
//				//*/
//			}
//			
//			
//			//sql = "DELETE FROM EMPLOYEE WHERE first = \"John\" and last = \"Smith\"";
//			//stmt.execute(sql);
//
//			//sql = "DROP TABLE IF EXISTS EMPLOYEE";
//			//stmt.execute(sql);
//
//			//STEP 6: Clean-up environment
//			rs.close();
//			stmt.close();
//			conn.close();
//		} catch(SQLException se) {
//			//Handle errors for JDBC
//			se.printStackTrace();
//		} catch (Exception e) {
//			//Handle errors for Class.forName
//			e.printStackTrace();
//		} finally {
//			//finally block used to close resources
//			try {
//				if(stmt!=null)
//					stmt.close();
//			} catch(SQLException se2) {
//			}
//			try {
//				if(conn!=null)
//					conn.close();
//			} catch(SQLException se) {
//				se.printStackTrace();
//			}
//		}
// 	    System.out.println("Done");
//	}
//	
//	
//	/**
//	 * Connects to a generic database. Can be used for mdbc or jdbc
//	 * @param DBC_DRIVER the driver for which to register (Class.forName(DBC_DRIVER))
//	 * @param DB_URL the URL for the database we are testing
//	 * @param connectionProps
//	 */
//	private void connectViaDriverManager(final String DBC_DRIVER, final String DB_URL, Properties connectionProps) {
//		Connection conn = null;
//		Statement stmt = null;
//		try {
//			
//			//Server server = Server.createTcpServer("-tcpAllowOthers").start();
//			//STEP 2: Register JDBC driver
//			Class.forName(DBC_DRIVER);
//
//			//STEP 3: Open a connection
//			conn = DriverManager.getConnection(DB_URL, connectionProps);
//			conn.setAutoCommit(false);
//			
//			//STEP 4: Execute a query
//			stmt = conn.createStatement();
//			String sql;
//			
//			//sql = "DROP TABLE EMPLOYEE";
//			//stmt.execute(sql);
//			
//			sql = "CREATE TABLE IF NOT EXISTS EMPLOYEE (id INT primary key, first VARCHAR(20), last VARCHAR(20), age INT);";
//			stmt.execute(sql);
//
//			sql = "INSERT INTO EMPLOYEE (id, first, last, age) VALUES (\"34\", \"Jane4\", \"Doe4\", \"40\")";
//			stmt.execute(sql);			
//
//			sql = "SELECT id, first, last, age FROM EMPLOYEE";
//			ResultSet rs = stmt.executeQuery(sql);
//
//			//STEP 5: Extract data from result set
//			while(rs.next()) {
//				//Retrieve by column name
//				int id  = rs.getInt("id");
//				int age = rs.getInt("age");
//				String first = rs.getString("first");
//				String last = rs.getString("last");
//
//				//Display values
//				//*
//				System.out.print("ID: " + id);
//				System.out.print(", Age: " + age);
//				System.out.print(", First: " + first);
//				System.out.println(", Last: " + last);
//				//*/
//
//			}
//			//sql = "DELETE FROM EMPLOYEE WHERE first = \"John\" and last = \"Smith\"";
//			//stmt.execute(sql);
//
//			//sql = "DROP TABLE IF EXISTS EMPLOYEE";
//			//stmt.execute(sql);
//
//			conn.commit();
//			
//			//STEP 6: Clean-up environment
//			rs.close();
//			stmt.close();
//			conn.close();
//		} catch(SQLException se) {
//			//Handle errors for JDBC
//			se.printStackTrace();
//		} catch (Exception e) {
//			//Handle errors for Class.forName
//			e.printStackTrace();
//		} finally {
//			//finally block used to close resources
//			try {
//				if(stmt!=null)
//					stmt.close();
//			} catch(SQLException se2) {
//			}
//			try {
//				if(conn!=null)
//					conn.close();
//			} catch(SQLException se) {
//				se.printStackTrace();
//			}
//		}
//	}
//
//	
//
//	/**
//	 * Must be mysql datasource
//	 * @throws Exception
//	 */
//	//@Test
//	public void test04() throws Exception {
//		String dbConnectionName = "testing";
//		String dbUserId = "alice";
//		String dbPasswd = "bob";
//		String db_url = "jdbc:mysql://localhost/EMP";
//		MysqlDataSource dataSource = new MysqlDataSource();
//		dataSource.setUser(dbUserId);
//		dataSource.setPassword(dbPasswd);
//		dataSource.setURL(db_url);
//
//
//		Connection con = dataSource.getConnection();
//		Statement st = con.createStatement();
//		ResultSet rs = null; 
//		
//		//FIXME CREATE EMPLOYEE TABLE
//		
//		if (st.execute("insert into EMPLOYEE values (\"John Doe\");")) {
//			rs = st.getResultSet();
//		}
//		
//		rs = st.executeQuery("select * from EMPLOYEE;");
//		while (rs.next()) {
//			System.out.println(rs.getString("name"));
//		}
//		
//		if (st.execute("DELETE FROM EMPLOYEE")) {
//			rs = st.getResultSet();
//		}
//		rs.close();
//		st.close();
//		con.close();
//	}
//	
//	/**
//	 * Test connection to mysql datasource class
//	 * @throws Exception
//	 */
//	@Test
//	public void test05() throws Exception {
//		String dbConnectionName = "testing";
//		String dbUserId = "alice";
//		String dbPasswd = "bob";
//		String db_url = "jdbc:mdbc://localhost/EMP";
//		String db_type = "mysql";
//		MdbcDataSource dataSource = new MdbcDataSource();
//		dataSource.setUser(dbUserId);
//		dataSource.setPassword(dbPasswd);
//		dataSource.setURL(db_url);
//		dataSource.setDBType(db_type);
//		
//		Connection con = dataSource.getConnection();
//		Statement st = con.createStatement();
//		ResultSet rs = null; 
//		
//		if (st.execute("insert into EMPLOYEE values (\"John Doe\");")) {
//			rs = st.getResultSet();
//		}
//		
//		rs = st.executeQuery("select * from EMPLOYEE;");
//		while (rs.next()) {
//			System.out.println(rs.getString("name"));
//		}
//		
//		if (st.execute("DELETE FROM EMPLOYEE")) {
//			rs = st.getResultSet();
//		}
//		rs.close();
//		st.close();
//		con.close();
//	}
}
