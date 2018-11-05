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
package org.onap.music.mdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.onap.music.mdbc.mixins.CassandraMixin;

public class TestCommon {
	public static final String DB_DRIVER   = "avatica.Driver";
	public static final String DB_USER     = "";
	public static final String DB_PASSWORD = "";

	public Connection getDBConnection(String url, String keyspace, String id) throws SQLException, ClassNotFoundException {
		Class.forName(DB_DRIVER);
		Properties driver_info = new Properties();
		driver_info.put(CassandraMixin.KEY_MY_ID,          id);
		driver_info.put(CassandraMixin.KEY_REPLICAS,       "0,1,2");
		driver_info.put(CassandraMixin.KEY_MUSIC_ADDRESS,  "localhost");
		driver_info.put("user",     DB_USER);
		driver_info.put("password", DB_PASSWORD);
		return DriverManager.getConnection(url, driver_info);
	}
}
