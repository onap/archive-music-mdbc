package com.att.research.mdbc;

public class Configuration {
    /** The property name to use to connect to cassandra*/
    public static final String KEY_CASSANDRA_URL = "CASSANDRA_URL";
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
