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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import org.onap.music.logging.EELFLoggerDelegate;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.onap.music.main.MusicCore;

/**
 * This class allows for management of the Cassandra Cluster and Session objects.
 *
 * @author Robert P. Eby
 */
public class MusicConnector {
	
	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicConnector.class);

	private Session session;
	private Cluster cluster;

	protected MusicConnector() {
		//to defeat instantiation since this is a singleton
	}

	public MusicConnector(String address) {
//		connectToCassaCluster(address);
		connectToMultipleAddresses(address);
	}

	public Session getSession() {
		return session;
	}

	public void close() {
		if (session != null)
			session.close();
		session = null;
		if (cluster != null)
			cluster.close();
		cluster = null;
	}
	
	private List<String> getAllPossibleLocalIps(){
		ArrayList<String> allPossibleIps = new ArrayList<String>();
		try {
			Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
			while(en.hasMoreElements()){
			    NetworkInterface ni=(NetworkInterface) en.nextElement();
			    Enumeration<InetAddress> ee = ni.getInetAddresses();
			    while(ee.hasMoreElements()) {
			        InetAddress ia= (InetAddress) ee.nextElement();
			        allPossibleIps.add(ia.getHostAddress());
			    }
			 }
		} catch (SocketException e) {
			e.printStackTrace();
		}
		return allPossibleIps;
	}
	
	private void connectToMultipleAddresses(String address) {
		MusicCore.getDSHandle(address);
	/*
	PoolingOptions poolingOptions =
		new PoolingOptions()
    	.setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
    	.setConnectionsPerHost(HostDistance.REMOTE, 2, 4);
	String[] music_hosts = address.split(",");
	if (cluster == null) {
		logger.info(EELFLoggerDelegate.applicationLogger,"Initializing MUSIC Client with endpoints "+address);
		cluster = Cluster.builder()
			.withPort(9042)
			.withPoolingOptions(poolingOptions)
			.withoutMetrics()
			.addContactPoints(music_hosts)
			.build();
		Metadata metadata = cluster.getMetadata();
		logger.info(EELFLoggerDelegate.applicationLogger,"Connected to cluster:"+metadata.getClusterName()+" at address:"+address);
		
	}
	session = cluster.connect();
	 */
	}

	@SuppressWarnings("unused")
	private void connectToCassaCluster(String address) {
		PoolingOptions poolingOptions =
			new PoolingOptions()
	    	.setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
	    	.setConnectionsPerHost(HostDistance.REMOTE, 2, 4);
		Iterator<String> it = getAllPossibleLocalIps().iterator();
		logger.info(EELFLoggerDelegate.applicationLogger,"Iterating through possible ips:"+getAllPossibleLocalIps());
		
		while (it.hasNext()) {
			try {
				cluster = Cluster.builder()
					.withPort(9042)
					.withPoolingOptions(poolingOptions)
					.withoutMetrics()
					.addContactPoint(address)
					.build();
				//cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(Integer.MAX_VALUE);
				Metadata metadata = cluster.getMetadata();
				logger.info(EELFLoggerDelegate.applicationLogger,"Connected to cluster:"+metadata.getClusterName()+" at address:"+address);
				
				session = cluster.connect();
				break;
			} catch (NoHostAvailableException e) {
				address = it.next();
			}
		}
	}
}
