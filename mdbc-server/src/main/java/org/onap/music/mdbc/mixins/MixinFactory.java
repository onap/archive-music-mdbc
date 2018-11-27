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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.util.Properties;

import org.onap.music.logging.EELFLoggerDelegate;

/**
 * This class is used to construct instances of Mixins that implement either the {@link org.onap.music.mdbc.mixins.DBInterface}
 * interface, or the {@link org.onap.music.mdbc.mixins.MusicInterface} interface. The Mixins are searched for in the CLASSPATH.
 *
 * @author Robert P. Eby
 */
public class MixinFactory {
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MixinFactory.class);

	// Only static methods...
	private MixinFactory(){}

	/**
	 * Look for a class in CLASSPATH that implements the {@link DBInterface} interface, and has the mixin name <i>name</i>.
	 * If one is found, construct and return it, using the other arguments for the constructor.
	 * @param name the name of the Mixin
	 * @param msm the MusicSqlManager to use as an argument to the constructor
	 * @param url the URL to use as an argument to the constructor
	 * @param conn the underlying JDBC Connection
	 * @param info the Properties to use as an argument to the constructor
	 * @return the newly constructed DBInterface, or null if one cannot be found.
	 */
	public static DBInterface createDBInterface(String name, MusicInterface mi, String url, Connection conn, Properties info) {
		for (Class<?> cl : Utils.getClassesImplementing(DBInterface.class)) {
			try {
				Constructor<?> con = cl.getConstructor();
				if (con != null) {
					DBInterface dbi = (DBInterface) con.newInstance();
					String miname = dbi.getMixinName();
					logger.info(EELFLoggerDelegate.applicationLogger,"Checking "+miname);
					if (miname.equalsIgnoreCase(name)) {
						con = cl.getConstructor(MusicInterface.class, String.class, Connection.class, Properties.class);
						if (con != null) {
							logger.info(EELFLoggerDelegate.applicationLogger,"Found match: "+miname);
							return (DBInterface) con.newInstance(mi, url, conn, info);
						}
					}
				}
			} catch (Exception e) {
				logger.error(EELFLoggerDelegate.errorLogger,"createDBInterface: "+e);
			}
		}
		return null;
	}
	/**
	 * Look for a class in CLASSPATH that implements the {@link MusicInterface} interface, and has the mixin name <i>name</i>.
	 * If one is found, construct and return it, using the other arguments for the constructor.
	 * @param name the name of the Mixin
	 * @param mdbcServerName the name of this mdbcServer instance
	 * @param info the Properties to use as an argument to the constructor
	 * @return the newly constructed MusicInterface, or null if one cannot be found.
	 */
	public static MusicInterface createMusicInterface(String name, String mdbcServerName, Properties info) {
		for (Class<?> cl : Utils.getClassesImplementing(MusicInterface.class)) {
			try {
				Constructor<?> con = cl.getConstructor();
				if (con != null) { //TODO: is this necessary? Don't think it could ever be null?
					MusicInterface mi = (MusicInterface) con.newInstance();
					String miname = mi.getMixinName();
					logger.info(EELFLoggerDelegate.applicationLogger, "Checking "+miname);
					if (miname.equalsIgnoreCase(name)) {
						con = cl.getConstructor(String.class, Properties.class);
						if (con != null) {
							logger.info(EELFLoggerDelegate.applicationLogger,"Found match: "+miname);
							return (MusicInterface) con.newInstance(mdbcServerName, info);
						}
					}
				}
			} catch (InvocationTargetException e) {
				logger.error(EELFLoggerDelegate.errorLogger,"createMusicInterface: "+e.getCause().toString());
			}
			catch (Exception e) {
				logger.error(EELFLoggerDelegate.errorLogger,"createMusicInterface: "+e);
			}
		}
		return null;
	}

	// Unfortunately, this version does not work when MDBC is built as a JBoss module,
	// where something funny is happening with the classloaders
//	@SuppressWarnings("unused")
//	private static List<Class<?>> getClassesImplementingOld(Class<?> implx) {
//		List<Class<?>> list = new ArrayList<Class<?>>();
//		try {
//			ClassLoader cldr = MixinFactory.class.getClassLoader();
//			while (cldr != null) {
//				ClassPath cp = ClassPath.from(cldr);
//				for (ClassPath.ClassInfo x : cp.getAllClasses()) {
//					if (x.toString().startsWith("com.att.")) {	// mixins must have a package starting with com.att.
//						Class<?> cl = x.load();
//						if (impl(cl, implx)) {
//							list.add(cl);
//						}
//					}
//				}
//				cldr = cldr.getParent();
//			}
//		} catch (IOException e) {
//			// ignore
//		}
//		return list;
//	}
	static boolean impl(Class<?> cl, Class<?> imp) {
		for (Class<?> c2 : cl.getInterfaces()) {
			if (c2 == imp) {
				return true;
			}
		}
		Class<?> c2 = cl.getSuperclass();
		return (c2 != null) ? impl(c2, imp) : false;
	}
}
