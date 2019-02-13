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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.json.JSONObject;

import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.TableInfo;
import com.datastax.driver.core.utils.Bytes;

/**
 * Utility functions used by several of the mixins should go here.
 *
 * @author Robert P. Eby
 */
public class Utils {
	public static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(Utils.class);
	
	/**
	 * Transforms and JsonObject into an array of objects
	 * @param ti information related to the table
	 * @param tbl table that jo belong to
	 * @param jo object that represents a row in the table
	 * @param musicDefaultPrimaryKeyName contains the name of key associated with the default primary key used by MUSIC, it can be null, if not requird 
	 * @return array with the objects in the row
	 */
	public static Object[] jsonToRow(TableInfo ti, String tbl, JSONObject jo, String musicDefaultPrimaryKeyName) {
		int columnSize = ti.columns.size();
		ArrayList<Object> rv = new ArrayList<Object>();
		if (musicDefaultPrimaryKeyName!=null && jo.has(musicDefaultPrimaryKeyName)) { 
			rv.add(jo.getString(musicDefaultPrimaryKeyName)); 
		}
		for (int i = 0; i < columnSize; i++) {
			String colname = ti.columns.get(i);
			switch (ti.coltype.get(i)) {
			case Types.BIGINT:
				rv.add(jo.optLong(colname, 0));
				break;
			case Types.BOOLEAN:
				rv.add(jo.optBoolean(colname, false));
				break;
			case Types.BLOB:
				rv.add(jo.optString(colname, ""));
				break;
			case Types.DECIMAL:
				rv.add(jo.optBigDecimal(colname, BigDecimal.ZERO));
				break;
			case Types.DOUBLE:
				rv.add(jo.optDouble(colname, 0));
				break;
			case Types.INTEGER:
				rv.add(jo.optInt(colname, 0));
				break;
			case Types.TIMESTAMP:
				//rv[i] = new Date(jo.optString(colname, ""));
				rv.add(jo.optString(colname, ""));
				break;
			case Types.DATE:
			case Types.VARCHAR:
				//Fall through
			default:
				rv.add(jo.optString(colname, ""));
				break;
			}
		}
		return rv.toArray();
	}
	
	/**
	 * Return a String equivalent of an Object.  Useful for writing SQL.
	 * @param val the object to String-ify
	 * @return the String value
	 */
	public static String getStringValue(Object val) {
		if (val == null)
			return "NULL";
		if (val instanceof String)
			return "'" + val.toString().replaceAll("'", "''") + "'";	// double any quotes
		if (val instanceof Number)
			return ""+val;
		if (val instanceof ByteBuffer)
			return "'" + Bytes.toHexString((ByteBuffer)val).substring(2) + "'";	// substring(2) is to remove the "0x" at front
		if (val instanceof Date)
			return "'" + (new Timestamp(((Date)val).getTime())).toString() + "'";
		// Boolean, and anything else
		return val.toString();
	}
	
	/**
	 * Parse result set and put into object array
	 * @param tbl
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	public static ArrayList<Object[]> parseResults(TableInfo ti, ResultSet rs) throws SQLException {
		ArrayList<Object[]> results = new ArrayList<Object[]>(); 
		while (rs.next()) {
			Object[] row = new Object[ti.columns.size()];
			for (int i = 0; i < ti.columns.size(); i++) {
				String colname = ti.columns.get(i);
				switch (ti.coltype.get(i)) {
				case Types.BIGINT:
					row[i] = rs.getLong(colname);
					break;
				case Types.BOOLEAN:
					row[i] = rs.getBoolean(colname);
					break;
				case Types.BLOB:
					System.err.println("WE DO NOT SUPPORT BLOBS IN H2!! COLUMN NAME="+colname);
					//logger.error("WE DO NOT SUPPORT BLOBS IN H2!! COLUMN NAME="+colname);
					// throw an exception here???
					break;
				case Types.DOUBLE:
					row[i] = rs.getDouble(colname);
					break;
				case Types.INTEGER:
					row[i] = rs.getInt(colname);
					break;
				case Types.TIMESTAMP:
					//rv[i] = new Date(jo.optString(colname, ""));
					row[i] = rs.getString(colname);
					break;
				case Types.VARCHAR:
					//Fall through
				default:
					row[i] = rs.getString(colname);
					break;
				}
			}
			results.add(row);
		}
		return results;
	}

	@SuppressWarnings("unused")
	static List<Class<?>> getClassesImplementing(Class<?> implx) {
		Properties pr = null;
		try {
			pr = new Properties();
			pr.load(Utils.class.getResourceAsStream("/mdbc.properties"));
		}
		catch (IOException e) {
			logger.error(EELFLoggerDelegate.errorLogger, "Could not load property file > " + e.getMessage());
		}
		
		List<Class<?>> list = new ArrayList<Class<?>>();
		if (pr==null) {
			return list;
		}
		String mixins = pr.getProperty("MIXINS");
		for (String className: mixins.split("[ ,]")) {
			try {
				Class<?> cl = Class.forName(className.trim());
				if (MixinFactory.impl(cl, implx)) {
					list.add(cl);
				}
			} catch (ClassNotFoundException e) {
				logger.error(EELFLoggerDelegate.errorLogger,"Mixin class "+className+" not found.");
			}
		}
		return list;
	}


}
