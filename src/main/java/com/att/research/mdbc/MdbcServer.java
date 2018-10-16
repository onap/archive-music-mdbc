package com.att.research.mdbc;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.att.research.mdbc.configurations.NodeConfiguration;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.util.Unsafe;

import com.att.research.logging.EELFLoggerDelegate;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.Locale;
import java.util.Properties;

public class MdbcServer {
  public static final EELFLoggerDelegate LOG = EELFLoggerDelegate.getLogger(MdbcStatement.class);

  @Parameter(names = { "-c", "--configuration" }, required = true,
      description = "This is the file that contains the ranges that are assigned to this MDBC server")
  private String configurationFile;

  @Parameter(names = { "-u", "--url" }, required = true,
      description = "JDBC driver url for the server")
  private String url;

  @Parameter(names = { "-p", "--port" }, required = true,
      description = "Port the server should bind")
  private int port;

  @Parameter(names = { "-s", "--user" }, required = true,
      description = "Mysql usr")
  private String user;

  @Parameter(names = { "-a", "--pass" }, required = true,
      description = "Mysql password")
  private String password;

  final private Serialization serialization = Serialization.PROTOBUF;

  @Parameter(names = { "-h", "-help", "--help" }, help = true,
      description = "Print the help message")
  private boolean help = false;

  private NodeConfiguration config;
  private HttpServer server;

  public void start() {
    if (null != server) {
      LOG.error("The server was already started");
      Unsafe.systemExit(ExitCodes.ALREADY_STARTED.ordinal());
      return;
    }

    try {
    	config = NodeConfiguration.readJsonFromFile(configurationFile);
    	//\TODO Add configuration file with Server Info
    	Properties connectionProps = new Properties();
    	connectionProps.put("user", user);
    	connectionProps.put("password", password);
    	MdbcServerLogic meta = new MdbcServerLogic(url,connectionProps,config);
    	LocalService service = new LocalService(meta);

    	// Construct the server
    	this.server = new HttpServer.Builder<>()
    			.withHandler(service, serialization)
    			.withPort(port)
    			.build();

    	// Then start it
    	server.start();

    	LOG.info("Started Avatica server on port {} with serialization {}", server.getPort(),
    			serialization);
    } catch (Exception e) {
    	LOG.error("Failed to start Avatica server", e);
    	Unsafe.systemExit(ExitCodes.START_FAILED.ordinal());
    }
  }

  public void stop() {
	  if (null != server) {
		  server.stop();
		  server = null;
	  }
  }

  public void join() throws InterruptedException {
	  server.join();
  }

  public static void main(String[] args) {
	  final MdbcServer server = new MdbcServer();
	  @SuppressWarnings("deprecation")
	  JCommander jc = new JCommander(server, args);
	  if (server.help) {
		  jc.usage();
		  Unsafe.systemExit(ExitCodes.USAGE.ordinal());
		  return;
	  }

	  server.start();

	  // Try to clean up when the server is stopped.
	  Runtime.getRuntime().addShutdownHook(
			  new Thread(new Runnable() {
				  @Override public void run() {
					  LOG.info("Stopping server");
					  server.stop();
					  LOG.info("Server stopped");
				  }
			  }));

	  try {
		  server.join();
	  } catch (InterruptedException e) {
		  // Reset interruption
		  Thread.currentThread().interrupt();
		  // And exit now.
		  return;
	  }
  }

  /**
   * Converter from String to Serialization. Must be public for JCommander.
   */
  public static class SerializationConverter implements IStringConverter<Serialization> {
	  @Override public Serialization convert(String value) {
		  return Serialization.valueOf(value.toUpperCase(Locale.ROOT));
	  }
  }

  /**
   * Codes for exit conditions
   */
  private enum ExitCodes {
	  NORMAL,
	  ALREADY_STARTED, // 1
	  START_FAILED,    // 2
	  USAGE;           // 3
  }
}

// End StandaloneServer.java
