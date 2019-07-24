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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class MdbcTestMultiClient implements Runnable {
    private static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
    
    private String connectionString = null;
    private int threadId = -1;
    
    private List<String> connectionStrings = new ArrayList<String>();
    private static final String defaultConnection = "jdbc:avatica:remote:url=http://localhost:30000/test;serialization=protobuf";
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
	private int skipInitialSelectPct = 25;
	private int selectInsteadOfUpdatePct = 25;
	private int rollbackChancePct = 15;
	private int maxTables = 0;
    public static boolean[] threadsDone;


	private boolean sequentialIds = false;
	private static Integer currentId = -1;
	
	private boolean sequentialFirsts = false;
	private static Integer currentFirstFirst = 0, currentFirstSecond = 0;
	
	private Long randomSeed = null;

	private List<String> tableNames = new ArrayList<String>();
    private static final List<String> defaultTableNames = Arrays.asList(new String[] {"persons", "persons2"});

	private boolean explainConnection = true;
	private boolean endInSelect = true;
	
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

    public MdbcTestMultiClient(String[] args) {
        char currState = (char)0, noState = (char)0;
        for (String arg : args) {
            if (currState==noState) {
                switch (arg) {
                    case "-?":
                    case "--help":
                        showHelp();
                        break;
                    case "-c":
                    case "--connection": 
                        currState = 'c';
                        break;
                    case "-e":
                    case "--tableName": 
                        currState = 'e';
                        break;
                    case "-n":
                    case "--name": 
                        currState = 'n';
                        break;
                    case "-b":
                    case "--baseId": 
                        currState = 'b';
                        break;
                    case "-r":
                    case "--baseRange": 
                        currState = 'r';
                        break;
                    case "-m":
                    case "--maxCalls": 
                        currState = 'm';
                        break;
                    case "-t":
                    case "--maxTime": 
                        currState = 't';
                        break;
                    case "-d":
                    case "--minDelay": 
                        currState = 'd';
                        break;
                    case "-a":
                    case "--addDelay": 
                        currState = 'a';
                        break;
                    case "-u":
                    case "--update": 
                    	doUpdate = false;
                        break;
                    case "-x":
                    case "--delete": 
                    	doDelete = false;
                    	break;
                    case "-l":
                    case "--closeChance": 
                        currState = 'l';
                        break;
                    case "-s":
                    case "--skipInitialSelect":
                        currState = 's';
                        break;
                    case "-i":
                    case "--selectNotUpdate":
                        currState = 'i';
                        break;
                    case "-o":
                    case "--rollbackChance":
                    	currState = 'o';
                    	break;
                    case "--maxTables":
                    	currState = '@';
                    	break;
                    case "--randomSeed":
                    	currState = '?';
                    	break;
                    case "--sequentialId":
                    	sequentialIds = true;
                    	break;
                    case "--sequentialFirst": 
                    	sequentialFirsts = true;
                    	break;
                    default:
                        System.out.println("Didn't understand switch " + arg);
                }
            } else {
                try {
                    switch (currState) {
                    	case 'c':
                    		connectionStrings.add(arg);
                    		break;
                        case 'e':
                            tableNames.add(arg);
                            break;
                        case 'n':
                            lastName = arg;
                            break;
                        case 'b':
                            baseId = Integer.parseInt(arg);
                            break;
                        case 'r':
                            baseIdRange = Integer.parseInt(arg);
                            break;
                        case 'm':
                            maxCalls = Integer.parseInt(arg);
                            break;
                        case 't':
                            maxTimeMs = Integer.parseInt(arg);
                            break;
                        case 'd':
                            minDelayBetweenTestsMs = Integer.parseInt(arg);
                            break;
                        case 'a':
                            additionalDelayBetweenTestsMs = Integer.parseInt(arg);
                            break;
                        case 'l':
                            connectionCloseChancePct = Integer.parseInt(arg);
                            break;
                        case 's':
                            skipInitialSelectPct = Integer.parseInt(arg);
                            break;
                        case 'i': 
                            selectInsteadOfUpdatePct = Integer.parseInt(arg);
                            break;
                        case 'o':
                            rollbackChancePct = Integer.parseInt(arg);
                            break;
                        case '@':
                        	maxTables = Integer.parseInt(arg);
                        	break;
                        case '?':
                        	randomSeed = Long.parseLong(arg);
                        	break;
                        default:
                            System.out.println("Bad state " + currState + "????");
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Bad integer " + arg + " for switch " + currState);
                }
                currState = noState;
            }
        }
        if (connectionStrings.isEmpty()) connectionStrings.add(defaultConnection);
        if (tableNames.isEmpty()) tableNames.addAll(defaultTableNames);
	}

	private void showHelp() {
	    System.out.println(
	            "-?; --help: Show help\n" + 
	                    "-c; --connection [string]: MDBC connection string, may appear multiple times\n" + 
	            		"-e; --tableName [string]: Table name, may appear multiple times\n" +
	                    "-n; --name [string]: Last name in persons table, default \"Lastname\"\n" + 
	                    "-b; --baseId [int]: Base ID, default 700\n" + 
	                    "-r; --baseRange [int]: Range of ID, default 50\n" + 
	                    "-m; --maxCalls [int]: Max number of commits (each may be 1+ updates), default 50\n" + 
	                    "-t; --maxTime [int]: Max time in ms test will run, default 60000\n" + 
	                    "-d; --minDelay [int]: Min delay between tests in ms, default 1000\n" + 
	                    "-a; --addDelay [int]: Max randomized additional delay between tests in ms, default 1000\n" + 
	                    "-u; --update: Don't generate update statements; default do\n" + 
	                    "-x; --delete: Don't generate delete statements; default do\n" + 
	                    "-l; --closeChance [int]: Percent chance of closing connection after each commit, default 50\n" +
	                    "-s; --skipInitialSelect [int]: Percent chance of skipping each initial select in a transaction, default 25\n" +
	                    "-i; --selectNotUpdate [int]: Percent chance of each action in a transaction being a select instead of an update, default 25\n" +
	                    "-o; --rollbackChance [int]: Percent chance of rolling back each transaction instead of committing, default 15\n" +
	                    "    --maxTables [int]: Maximum number of tables per transaction, default 0 (no limit)\n" +
	                    "    --randomSeed [long]: Seed for the initial random number generator, default none (generate random random seed)\n" +
	                    "    --sequentialId: Generate sequential IDs instead of random ones (default random)\n" +
	                    "    --sequentialFirst: Generate alphabetically sequential first names (default completely random) \n" +
	                    ""
	    );
        
    }

    public MdbcTestMultiClient(MdbcTestMultiClient that, int i) {
        this.connectionString = that.connectionStrings.get(i);
        this.tableNames = that.tableNames;
        this.threadId = i;
        
        this.lastName = that.lastName;
        this.lastName = that.lastName;
        this.baseId = that.baseId;
        this.baseIdRange = that.baseIdRange;
        this.maxCalls = that.maxCalls;
        this.maxTimeMs = that.maxTimeMs;
        this.minDelayBetweenTestsMs = that.minDelayBetweenTestsMs;
        this.additionalDelayBetweenTestsMs = that.additionalDelayBetweenTestsMs;
        this.doDelete = that.doDelete;
        this.doUpdate = that.doUpdate;
        this.connectionCloseChancePct = that.connectionCloseChancePct;
        this.skipInitialSelectPct = that.skipInitialSelectPct;
        this.selectInsteadOfUpdatePct = that.selectInsteadOfUpdatePct;
        this.rollbackChancePct = that.rollbackChancePct;
        this.maxTables = that.maxTables;
        this.sequentialIds = that.sequentialIds;
        this.sequentialFirsts = that.sequentialFirsts;
    }

	private void setRandomSeed(Long randomSeed) {
		this.randomSeed = randomSeed;
	}

	private void doTest(Connection connection, Random r) throws SQLException {
	    doLog("skipInitialSelectPct = " + skipInitialSelectPct + ", selectInsteadOfUpdatePct = " + selectInsteadOfUpdatePct);
	    HashMap<String, HashMap<Integer, Employee>> employeeMaps = new HashMap<String, HashMap<Integer, Employee>> ();
//    	HashMap<Integer, Employee> employeeMap = new HashMap<Integer, Employee>();

	    List<String> myTableNames = chooseTableNames(r);
	    
	    for (String tableName : myTableNames) {
	        if (r.nextInt(100)<skipInitialSelectPct) {
	            doLog("Skipping select");
//	            employeeMap = null;
	        } else {
	            Statement querySt = connection.createStatement();
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

	            HashMap<Integer, Employee> employeeMap = new HashMap<Integer, Employee>();
	            employeeMaps.put(tableName, employeeMap);
	            
	            ResultSet rs = executeQueryTimed("select * from " + tableName, querySt, false);
	            while (rs.next()) {
	                //       		doLog("PersonId = " + rs.getInt("personId") + ", lastname = " + rs.getString("lastname") + ", firstname = " + rs.getString("firstname"));
	                Employee emp = new Employee(rs.getInt("personId"), rs.getString("lastname"), rs.getString("firstname"), rs.getString("address"), rs.getString("city"));
	                employeeMap.put(rs.getInt("personId"), emp);
	                if (sequentialIds) updateId(rs.getInt("personId"));
	                if (sequentialFirsts) updateFirst(rs.getString("firstname"));
	                doLog("Found: " + emp);
	            }
	            querySt.close();
	        }
	    }

    	Statement insertStmt = connection.createStatement();
    	boolean firstTry = true;
//    	String tableName = tableNames[r.nextInt(tableNames.length)];
//    	HashMap<Integer, Employee> employeeMap = employeeMaps.get(tableName);
//    	executeUpdateTimed(generateStatement(employeeMap, r, tableName), insertStmt, true);

    	while (firstTry || r.nextBoolean()) {
    	    firstTry = false;
            String tableName = myTableNames.get(r.nextInt(myTableNames.size()));
            HashMap<Integer, Employee> employeeMap = employeeMaps.get(tableName);
            if (r.nextInt(100)<selectInsteadOfUpdatePct) {
                Statement querySt = connection.createStatement();
                ResultSet rs = executeQueryTimed("select count(*) x from " + tableName, querySt, false);
                if (rs.next()) doLog("Select count returned " + rs.getInt("x")); else doLog("Didn't get response from select count!");
                rs.close();
                querySt.close();
            } else {
                executeUpdateTimed(generateStatement(employeeMap, r, tableName), insertStmt, true);
            }
    	}

    	if (r.nextInt(100)<rollbackChancePct) {
    		doLog("Rollback!");
    		connection.rollback();
    	} else {
    		doLog("Commit");
    		connection.commit();
    	}
    	
    	insertStmt.close();
    }
    
    private void updateFirst(String firstName) {
    	if (firstName==null || firstName.length()<2) return;
    	synchronized(currentFirstFirst) {
//			return (char)(65+currentFirstFirst) + "" + (char)(97+currentFirstSecond) + generateLetters(r, 4+r.nextInt(4));
    		int ff = ((int)firstName.charAt(0))-65;
    		int fs = ((int)firstName.charAt(1))-97;
    		if (ff>=26 || ff<0 || fs>=26 || fs<0) return;
    		if ( (ff>currentFirstFirst) || (ff==currentFirstFirst && fs>currentFirstSecond) ) {
    			currentFirstFirst = ff;
    			currentFirstSecond = fs;
    			doLog("Saw " + firstName + ", updating currentFirstName to " + currentFirstFirst + ", " + currentFirstSecond);
    		}
    	}
		
	}

	private void updateId(int id) {
    	synchronized(currentId) {
    		if (currentId<=id) {
    			currentId = id+1;
    			doLog ("Saw " + id + ", updating current id");
    		}
    	}
	}

	private List<String> chooseTableNames(Random r) {
    	if (maxTables<=0 || maxTables>=tableNames.size()) return tableNames;
    	boolean[] useTable = new boolean[tableNames.size()];
    	for (int i=0; i<tableNames.size(); i++) useTable[i] = false;
    	for (int i=0; i<maxTables; i++) useTable[r.nextInt(tableNames.size())] = true;
    	List<String> toRet = new ArrayList<String>();
    	for (int i=0; i<tableNames.size(); i++) if (useTable[i]) toRet.add(tableNames.get(i));
    	doLog("Selected tables: " + toRet);
		return toRet;
	}

	private void executeUpdateTimed(String sql, Statement insertStmt, boolean swallowException) throws SQLException {
        long beforeTime, afterTime;

        beforeTime = new java.util.Date().getTime();
        try {
            insertStmt.execute(sql);
        } catch (org.apache.calcite.avatica.AvaticaSqlException e) {
            if (swallowException) {
                doLog("Caught exception: " + e);
                e.printStackTrace();
            } else {
                throw e;
            }
        }
        afterTime = new java.util.Date().getTime();
        doLog("After update, execution time = " + (afterTime-beforeTime) + " ms");
    }

    private ResultSet executeQueryTimed(String sql, Statement querySt, boolean swallowException) throws SQLException {
        long beforeTime, afterTime;
        
        doLog("Before select: " + sql);
        beforeTime = new java.util.Date().getTime();
        ResultSet rs = null;
        try {
            rs = querySt.executeQuery(sql);
        } catch (SQLException e) {
            if (swallowException) {
                doLog("Caught exception: " + e);
            } else {
                throw e;
            }
        }
        afterTime = new java.util.Date().getTime();
        doLog("After select, execution time = " + (afterTime-beforeTime) + " ms");
        return rs;
    }

    private String generateStatement(HashMap<Integer, Employee> employeeMap, Random r, String tableName) {
    	String toRet = null;
    	
    	boolean pickInsert = ((employeeMap==null) || (r.nextInt(8) >= employeeMap.size()));
    	if (!pickInsert) {
    	    if (doDelete && doUpdate) {
    	        if (r.nextBoolean()) toRet = generateDelete(employeeMap, r, tableName); else toRet = generateUpdate(employeeMap, r, tableName);
    	    } else if (doDelete) {
    	        toRet = generateDelete(employeeMap, r, tableName);
    	    } else if (doUpdate) {
    	        toRet = generateUpdate(employeeMap, r, tableName);
    	    }
    	}
    	if (toRet==null) {
    		toRet = generateInsert(employeeMap, r, tableName);
    	}
    	
    	doLog("Generated statement: " + toRet);
    	
		return toRet;
	}

	private String generateInsert(HashMap<Integer, Employee> employeeMap, Random r, String tableName) {
		String toRet = null;
		
		Integer id = generateId(employeeMap, r);
		Employee newEmp = new Employee(id, lastName, generateFirstName(r), generateLetters(r, 4).toUpperCase(), generateLetters(r, 4).toUpperCase());
//		Employee newEmp = new Employee(id, lastName, Character.toUpperCase(randomLetter(r)) + generateLetters(r, 4+r.nextInt(4)), generateLetters(r, 4).toUpperCase(), generateLetters(r, 4).toUpperCase());
		toRet = "insert into " + tableName + " values (" + id + ", '" + newEmp.getLastname() + "', '" + newEmp.getFirstname() + "', '" + newEmp.getAddress() + "', '" + newEmp.getCity() + "')";
		if (employeeMap!=null) employeeMap.put(id, newEmp);
		
		return toRet;
	}

	private String generateFirstName(Random r) {
		if (sequentialFirsts) {
			synchronized(currentFirstFirst) {
				currentFirstSecond++;
				if (currentFirstSecond==26) {
					currentFirstSecond = 0;
					currentFirstFirst++;
					if (currentFirstFirst==26) currentFirstFirst=0;
				}
				return (char)(65+currentFirstFirst) + "" + (char)(97+currentFirstSecond) + generateLetters(r, 4+r.nextInt(4));
			}
		} else {
			return Character.toUpperCase(randomLetter(r)) + generateLetters(r, 4+r.nextInt(4));
		}
	}

	private Integer generateId(HashMap<Integer, Employee> employeeMap, Random r) {
		Integer toRet = null;
		if (currentId<0 && baseId>0) currentId = baseId; // setup, only matters if sequentialIds is true
		int range = baseIdRange; // setup, only matters if sequentialIds is false
		while (toRet==null) {
			if (sequentialIds) {
				synchronized(currentId) {
					toRet = currentId++;
				}
			} else {
				toRet = baseId + r.nextInt(range);
				if (employeeMap==null) toRet+=baseIdRange;
				range+=(baseIdRange/5);
			}
			if (employeeMap!=null && employeeMap.containsKey(toRet)) toRet = null;
		}
		return toRet;
	}

	private String generateUpdate(HashMap<Integer, Employee> employeeMap, Random r, String tableName) {
		String toRet = null;
		
		Employee toUpd = chooseTarget(employeeMap, r);
		if (toUpd!=null) {
			String newFirst = null;
			if (sequentialFirsts) {
				newFirst = generateFirstName(r);
			} else if (toUpd.getFirstname().length()<=3 || r.nextBoolean()) {
				newFirst = toUpd.getFirstname() + randomLetter(r);
			} else {
				newFirst = toUpd.getFirstname().substring(0, toUpd.getFirstname().length()-1);
			}
			toRet = "update " + tableName + " set firstname = '" + newFirst + "' where personid = " + toUpd.getEmpid() + " and lastname = '" + toUpd.getLastname() + "'";
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

	private String generateDelete(HashMap<Integer, Employee> employeeMap, Random r, String tableName) {
		String toRet = null;
		
		Employee toDel = chooseTarget(employeeMap, r);
		if (toDel!=null) {
			toRet = "delete from " + tableName + " where personid = " + toDel.getEmpid() + " and lastname = '" + toDel.getLastname() + "'";
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

	public void run() {
	    try {
            Class.forName("org.apache.calcite.avatica.remote.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
		
        Connection connection = null;

        int calls = 0;
        long startTime = new java.util.Date().getTime();
        Random r = null;
        if (randomSeed==null) {
        	r = new Random();
        	doLog("Generated new rng");
        } else {
        	r = new Random(randomSeed);
        	doLog("Generated new rng with seed " + randomSeed);
        }
        boolean done = false;

        while (!done) {
        	if (connection==null) {
                try {
                	doLog("Opening new connection");
                    connection = DriverManager.getConnection(connectionString);
                    connection.setAutoCommit(false);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }
        	} else {
        		doLog("Keeping open connection");
        	}

        	if (calls==0) {
                long initialDelay = 1 + (1000*threadId);
//                initialDelay = 1;
                synchronized(Thread.currentThread()) {
                    doLog("Delaying for " + initialDelay);
                    try {
                        Thread.currentThread().wait(initialDelay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        done = true;
                    }
                    doLog("Done");
                }
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
                	doLog("Connection closed.");
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
        		if (delay>0) {
        		    doLog("Delaying for " + delay + " ms");
        		    synchronized(Thread.currentThread()) {
        		        try {
        		            Thread.currentThread().wait(delay);
        		        } catch (InterruptedException e) {
        		            e.printStackTrace();
        		            done = true;
        		        }
        		    }
                    doLog("Delaying done");
        		}
        	}
        	
        	doLog("");
        }
        threadsDone[threadId] = true;

        if (endInSelect) {
            doLog("Ending in select to ensure all db's are in sync");
            while (!allThreadsDone()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    continue;
                }
            }
            doLog("All threads are done. Ending in select");
            if (connection==null) {
                try {
                    doLog("Opening new connection");
                    connection = DriverManager.getConnection(connectionString);
                    connection.setAutoCommit(false);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }
            }
            for (String tableName : tableNames) {
                try {
                    Statement querySt = connection.createStatement();
                    ResultSet rs = executeQueryTimed("select * from " + tableName, querySt, false);
                    while (rs.next()) {
                        //              doLog("PersonId = " + rs.getInt("personId") + ", lastname = " + rs.getString("lastname") + ", firstname = " + rs.getString("firstname"));
                        Employee emp = new Employee(rs.getInt("personId"), rs.getString("lastname"), rs.getString("firstname"), rs.getString("address"), rs.getString("city"));
                        doLog("Found: " + emp);
                    }
                    querySt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
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

	private boolean allThreadsDone() {
        for (Boolean b: this.threadsDone) {
            if (!b) {
                return false;
            }
        }
        return true;
    }

    private void doLog(String string) {
		System.out.println(">> Thread " + threadId + " " + sdf.format(new java.util.Date()) + " >> " + string);
	}
	
	public static void main(String[] args) throws InterruptedException {
		MdbcTestMultiClient mtc = new MdbcTestMultiClient(args);
		mtc.runTests(); 
	}

    private void runTests() throws InterruptedException {
    	if (randomSeed==null) {
    		randomSeed = new Random().nextLong();
    	}
		doLog("Using random seed = " + randomSeed);
    	Random seedRandom = new Random(randomSeed);
    	this.threadsDone = new boolean[connectionStrings.size()];

        for (int i=0; i<connectionStrings.size(); i++) {
            MdbcTestMultiClient mt = new MdbcTestMultiClient(this, i);
            mt.setRandomSeed(seedRandom.nextLong());
            Thread t = new Thread(mt);
            t.start();
        }
        
    }
		
}
