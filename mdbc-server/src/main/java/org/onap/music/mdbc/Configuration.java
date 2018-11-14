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

public class Configuration {
    /** The property name to use to connect to cassandra*/
    public static final String KEY_CASSANDRA_URL = "cassandra.host";
    /** The property name to use to enable/disable the MusicSqlManager entirely. */
    public static final String KEY_DISABLED         = "disabled";
    /** The property name to use to select the DB 'mixin'. */
    public static final String KEY_DB_MIXIN_NAME    = "MDBC_DB_MIXIN";
    /** The property name to use to select the MUSIC 'mixin'. */
    public static final String KEY_MUSIC_MIXIN_NAME = "MDBC_MUSIC_MIXIN";
    /** The name of the default mixin to use for the DBInterface. */
    public static final String DB_MIXIN_DEFAULT     = "mysql";//"h2";
    /** The name of the default mixin to use for the MusicInterface. */
    public static final String MUSIC_MIXIN_DEFAULT  = "cassandra2";//"cassandra2";
    /** Default cassandra ulr*/
    public static final String CASSANDRA_URL_DEFAULT = "localhost";//"cassandra2";
}
