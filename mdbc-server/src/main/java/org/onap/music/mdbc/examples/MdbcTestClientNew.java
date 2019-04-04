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
package org.onap.music.mdbc.examples;

import java.sql.*;
import java.util.HashMap;
import java.util.Random;

import org.apache.calcite.avatica.remote.Driver;

public class MdbcTestClientNew {
	private String lastName = "Lastname";
	private int baseId = 700;
	private int baseIdRange = 50;
	private int maxCalls = 50; 
	private int maxTimeMs = 60000;
	private int minDelayBetweenTestsMs = 1000;
	private int additionalDelayBetweenTestsMs = 1000;
	private boolean doDelete = true;
	private boolean doUpdate = true;
	private int connectionCloseChancePct = 50;
	
	private boolean explainConnection = true;
	
    public static class Employee {
        public final int empid;
        public String lastname;
        public String firstname;
        public String address;
        public String city;
        
		public Employee(int empid, String lastname, String firstname, String address, String city) {
			super();
			this.empid = empid;
			this.lastname = lastname;
			this.firstname = firstname;
			this.address = address;
			this.city = city;
		}

		public String getLastname() {
			return lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public String getAddress() {
			return address;
		}

		public void setAddress(String address) {
			this.address = address;
		}

		public String getCity() {
			return city;
		}

		public void setCity(String city) {
			this.city = city;
		}

		public int getEmpid() {
			return empid;
		}

		@Override
		public String toString() {
			return "Employee: " + empid + ", " + lastname + ", " + firstname + ", " + address + ", " + city;
		}

        
    }

    public MdbcTestClientNew(String[] args) {
    	lastName = args[0];
    	baseId = Integer.parseInt(args[1]);
    	baseIdRange = Integer.parseInt(args[2]);
    	maxCalls = Integer.parseInt(args[3]);
    	maxTimeMs = Integer.parseInt(args[4]);
    	minDelayBetweenTestsMs = Integer.parseInt(args[5]);
    	additionalDelayBetweenTestsMs = Integer.parseInt(args[6]);
    	doDelete = (args[7].toUpperCase().startsWith("Y")); 
    	doUpdate = (args[8].toUpperCase().startsWith("Y"));
    	connectionCloseChancePct = Integer.parseInt(args[9]);
	}

	public MdbcTestClientNew() {
		// Use default values
	}

	private void doTest(Connection connection, Random r) throws SQLException {
    	HashMap<Integer, Employee> employeeMap = new HashMap<Integer, Employee>();

    	Statement querySt = connection.createStatement();
    	doLog("Before select");

    	if (explainConnection) {
    		doLog("querySt is a: ");
    		Class<?> qsClass = querySt.getClass();
    		while (qsClass!=null) {
    			doLog(">>> " + qsClass.getName());
    			qsClass = qsClass.getSuperclass();
    		}
    		doLog("connection is a ");
    		qsClass = connection.getClass();
    		while (qsClass!=null) {
    			doLog(">>> " + qsClass.getName());
    			qsClass = qsClass.getSuperclass();
    		}
    		explainConnection = false;
    	}
    	
    	ResultSet rs = querySt.executeQuery("select * from persons");
    	doLog("After select");
    	while (rs.next()) {
//       		doLog("PersonId = " + rs.getInt("personId") + ", lastname = " + rs.getString("lastname") + ", firstname = " + rs.getString("firstname"));
    		Employee emp = new Employee(rs.getInt("personId"), rs.getString("lastname"), rs.getString("firstname"), rs.getString("address"), rs.getString("city"));
    		employeeMap.put(rs.getInt("personId"), emp);
    		doLog("Found: " + emp);
    	}
    	querySt.close();

    	Statement insertStmt = connection.createStatement();

    	insertStmt.execute(generateStatement(employeeMap, r));
    	while (r.nextBoolean()) {
    		insertStmt.execute(generateStatement(employeeMap, r));
    	}

    	connection.commit();

    	insertStmt.close();
    }
    
    private String generateStatement(HashMap<Integer, Employee> employeeMap, Random r) {
    	String toRet = null;
    	
    	int which = r.nextInt(3);
    	if (which==0 && doDelete) {
    		toRet = generateDelete(employeeMap, r);
    	} else if (which==1 && doUpdate) {
    		toRet = generateUpdate(employeeMap, r);
    	} 
    	if (toRet==null) {
    		toRet = generateInsert(employeeMap, r);
    	}
    	
    	doLog("Generated statement: " + toRet);
    	
		return toRet;
	}

	private String generateInsert(HashMap<Integer, Employee> employeeMap, Random r) {
		String toRet = null;
		
		Integer id = null;
		int range = baseIdRange;
		while (id==null) {
			id = baseId + r.nextInt(range);
			if (employeeMap.containsKey(id)) id = null;
			range+=(baseIdRange/5);
		}
		Employee newEmp = new Employee(id, lastName, Character.toUpperCase(randomLetter(r)) + generateLetters(r, 4+r.nextInt(4)), generateLetters(r, 4).toUpperCase(), generateLetters(r, 4).toUpperCase());
		toRet = "insert into persons values (" + id + ", '" + newEmp.getLastname() + "', '" + newEmp.getFirstname() + "', '" + newEmp.getAddress() + "', '" + newEmp.getCity() + "')";
		employeeMap.put(id, newEmp);
		
		return toRet;
	}

	private String generateUpdate(HashMap<Integer, Employee> employeeMap, Random r) {
		String toRet = null;
		
		Employee toUpd = chooseTarget(employeeMap, r);
		if (toUpd!=null) {
			String newFirst = null;
			if (toUpd.getFirstname().length()<=3 || r.nextBoolean()) {
				newFirst = toUpd.getFirstname() + randomLetter(r);
			} else {
				newFirst = toUpd.getFirstname().substring(0, toUpd.getFirstname().length()-1);
			}
//			toRet = "update persons set firstname = '" + newFirst + "' where personid = " + toUpd.getEmpid();
			toRet = "update persons set firstname = '" + newFirst + "' where personid = " + toUpd.getEmpid() + " and lastname = '" + toUpd.getLastname() + "'";
			toUpd.setFirstname(newFirst);
		}
		
		return toRet;
	}

	private String generateLetters(Random r, int count) {
		StringBuffer toRet = new StringBuffer();
		for (int i=0; i<count; i++) {
			Character c = null;
			while (c==null) {
				c = randomLetter(r);
				char cc = c.charValue();
				if ( (cc=='a' || cc=='e' || cc=='i' || cc=='o' || cc=='u') ^ (i%2==0) ) c = null;
			}
			toRet.append(c);
		}
		return toRet.toString();
	}

	private char randomLetter(Random r) {
		int a = (int)'a';
		return (char)(a+r.nextInt(26));
	}

	private String generateDelete(HashMap<Integer, Employee> employeeMap, Random r) {
		String toRet = null;
		
		Employee toDel = chooseTarget(employeeMap, r);
		if (toDel!=null) {
			toRet = "delete from persons where personid = " + toDel.getEmpid() + " and lastname = '" + toDel.getLastname() + "'";
			employeeMap.remove(toDel.getEmpid());
		}
		
		return toRet;
	}

	
	
	private Employee chooseTarget(HashMap<Integer, Employee> employeeMap, Random r) {
		Employee toPick = null;
		int count = 0;
		for (int id : employeeMap.keySet()) {
			Employee emp = employeeMap.get(id);
			if (!emp.getLastname().equals(lastName)) continue;
			count++;
			if (r.nextInt(count)==0) toPick = emp;
		}
		return toPick;
	}

	public void runTests() {
		try {
            Class.forName("org.apache.calcite.avatica.remote.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        Connection connection = null;

        Random r = new Random();
        boolean done = false;
        int calls = 0;
        long startTime = new java.util.Date().getTime();
        while (!done) {
        	if (connection==null) {
                try {
                	doLog("Opening new connection");
                    connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:30000/test;serialization=protobuf");
                    connection.setAutoCommit(false);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }
        	} else {
        		doLog("Keeping open connection");
        	}
        	
        	try {
				doLog("Running test");
				doTest(connection, r);
				doLog("Test complete");
			} catch (SQLException e1) {
				e1.printStackTrace();
				done = true;
				if (connection!=null) {
					try {
						doLog("Closing connection in catch block");
						connection.close();
					} catch (SQLException e) {
						e.printStackTrace();
						done = true;
					} finally {
			 			connection = null;
					}
				}
			}

        	if (!done && connection!=null && r.nextInt(100)<connectionCloseChancePct) {
        		try {
        			doLog("Closing connection");
        			connection.close();
        			connection = null;
        		} catch (SQLException e) {
        			e.printStackTrace();
        			done = true;
        		}
        	} else {
        		doLog("Not closing connection");
        	}
        	
        	calls++;
        	long msElapsed = (new java.util.Date().getTime()) - startTime;
        	if (calls>maxCalls || msElapsed > maxTimeMs) done = true;
        	
        	if (!done) {
        		long delay = r.nextInt(minDelayBetweenTestsMs);
        		while (r.nextBoolean()) delay += r.nextInt(additionalDelayBetweenTestsMs);
        		synchronized(r) {
        			try {
						r.wait(delay);
					} catch (InterruptedException e) {
						e.printStackTrace();
						done = true;
					}
        		}
        	}
        	
        	doLog("");
        }

        if (connection!=null) {
        	try {
        		doLog("Closing connection at end");
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }

        doLog("All done.");
    }

	private void doLog(String string) {
		System.out.println(">>> " + string);
	}
	
	public static void main(String[] args) {
		MdbcTestClientNew mtc = null;
		if (args.length==0) {
			mtc = new MdbcTestClientNew();
		} else if (args.length==10) {
			mtc = new MdbcTestClientNew(args);
		} else {
			System.out.println("Usage: [this] lastname baseId baseIdRange maxCalls maxTimeMs minDelayBetweenTestsMs additionalDelayBetweenTestsMs doDelete doUpdate connectionCloseChancePct");
			System.out.println(" lastname: Lastname for all inserts/updates/deletes");
			System.out.println(" baseId/baseRange: Id for all inserts will be between baseId and (baseId+baseRange).  In case of collision baseRange will be increased until available Id is found.");
			System.out.println(" maxCalls/maxTimeMs: Maximum number of commits (each of which may be 1+ updates) or time (in ms) that the test will run, whichever comes first"); 
			System.out.println(" minDelayBetweenTestsMs/additionalDelayBetweenTestsMs: After each test, delay for minDelayBetweenTestsMs ms plus (0 or more) times additionalDelayBetweenTestsMs ms");
			System.out.println(" doUpdate/doDelete: If \"Y\", will try to generate updates and deletes in addition to inserts.  Any failures to generate an update/delete will be replaced with an insert.");
			System.out.println(" connectionCloseChancePct: after each commit, percent chance of closing connection and opening a new one.");
			System.out.println("Default settings: Lastname 700 50 50 60000 1000 1000 Y Y 50");
		}
		
		mtc.runTests(); 
	}
		
}
