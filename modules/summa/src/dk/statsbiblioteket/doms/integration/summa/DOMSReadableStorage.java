/*
 * $Id$
 * $Revision$
 * $Date$
 * $Author$
 *
 * The DOMS project.
 * Copyright (C) 2007-2010  The State and University Library
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package dk.statsbiblioteket.doms.integration.summa;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import dk.statsbiblioteket.summa.common.configuration.SubConfigurationsNotSupportedException;
import dk.statsbiblioteket.summa.storage.api.QueryOptions;
import dk.statsbiblioteket.summa.storage.api.ReadableStorage;

/**
 * @author &lt;tsh@statsbiblioteket.dk&gt;
 * 
 */
public class DOMSReadableStorage implements ReadableStorage {

    private final Configuration configuration;
    private final DOMSWSClient domsClient;
    private final Map<String, URI> baseCollectionID;
    private final Map<String, URI> baseEntryContentModelID;

    /**
     * 
     * @param configuration
     * @throws ConfigurationException
     */
    public DOMSReadableStorage(Configuration configuration)
	    throws ConfigurationException {
	this.configuration = configuration;
	baseCollectionID = new HashMap<String, URI>();
	baseEntryContentModelID = new HashMap<String, URI>();
	initIDMaps(configuration, baseCollectionID, baseEntryContentModelID);
	domsClient = domsLogin(configuration);
    }

    /**
     * Get the time-stamp for when the latest modification occurred in the DOMS
     * collection identified by <code>base</code>. Please see the interface
     * documentation for further details.
     * 
     * @param base
     *            ID of the collection to read from. I.e. the PID of the DOMS
     *            collection.
     * @return The time-stamp in milliseconds for the latest modification made
     *         in the collection identified by <code>base</code>.
     * 
     * @see dk.statsbiblioteket.summa.storage.api.ReadableStorage#getModificationTime(java.lang.String)
     */
    @Override
    public long getModificationTime(String base) throws IOException {
	try {
	    final String collectionPID = baseCollectionID.get(base).toString();
	    return domsClient.getModificationTime(collectionPID);
	} catch (Exception exception) {
	    throw new IOException(
		    "Failed retrieving the modification time for base: " + base,
		    exception);
	}
    }

    /* (non-Javadoc)
     * @see dk.statsbiblioteket.summa.storage.api.ReadableStorage#getRecordsModifiedAfter(long, java.lang.String, dk.statsbiblioteket.summa.storage.api.QueryOptions)
     */
    @Override
    public long getRecordsModifiedAfter(long time, String base,
	    QueryOptions options) throws IOException {
	// TODO Auto-generated method stub
	return 0;
    }

    /* (non-Javadoc)
     * @see dk.statsbiblioteket.summa.storage.api.ReadableStorage#next(long, int)
     */
    @Override
    public List<Record> next(long arg0, int arg1) throws IOException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Record getRecord(String arg0, QueryOptions arg1) throws IOException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public List<Record> getRecords(List<String> arg0, QueryOptions arg1)
	    throws IOException {
	return new ArrayList<Record>();
    }

    @Override
    public Record next(long arg0) throws IOException {
	// TODO Auto-generated method stub
	return null;
    }

    /**
     * 
     * @param configuration
     * @return
     * @throws ConfigurationException
     */
    private DOMSWSClient domsLogin(Configuration configuration)
	    throws ConfigurationException {

	final String userName = configuration
	        .getString(ConfigurationKeys.DOMS_USER_NAME);

	final String password = configuration
	        .getString(ConfigurationKeys.DOMS_PASSWORD);

	if (userName == null || password == null) {
	    throw new ConfigurationException(
		    "Invalid DOMS user credentials in the configuration. username = '"
		            + userName + "'  password = '" + password + "'");
	}

	final String domsWSEndpointURL = configuration
	        .getString(ConfigurationKeys.DOMS_API_WEBSERVICE_URL);
	try {
	    final DOMSWSClient newDomsClient = new DOMSWSClient();
	    final URL domsWSAPIEndpoint = new URL(domsWSEndpointURL);
	    newDomsClient.login(domsWSAPIEndpoint, userName, password);
	    return newDomsClient;
	} catch (MalformedURLException malformedURLException) {
	    throw new ConfigurationException(
		    "Failed connecting to the DOMS API webservice with the URL"
		            + " (" + domsWSEndpointURL
		            + ") specified in the configuration.",
		    malformedURLException);
	}
    }

    private void initIDMaps(Configuration configuration,
	    Map<String, URI> baseCollectionIDMap,
	    Map<String, URI> baseEntryContentModelIDMap)
	    throws ConfigurationException {
	String baseID = null;
	try {
	    final List<Configuration> baseConfigurations = configuration
		    .getSubConfigurations(ConfigurationKeys.ACCESSIBLE_COLLECTION_BASES);

	    for (Configuration subConfiguration : baseConfigurations) {
		baseID = subConfiguration
		        .getString(ConfigurationKeys.COLLECTION_BASE_ID);

		final String collectionID_URI = subConfiguration
		        .getString(ConfigurationKeys.COLLECTION_ID);
		baseCollectionIDMap.put(baseID, new URI(collectionID_URI));

		final String collectionContentModelURI = subConfiguration
		        .getString(ConfigurationKeys.COLLECTION_ENTRY_CONTENT_MODEL_ID);
		baseEntryContentModelIDMap.put(baseID, new URI(
		        collectionContentModelURI));
	    }
	} catch (Exception exception) {
	    throw new ConfigurationException(
		    "Could not retrieve the collection base (base ID = '"
		            + baseID + "' configuration information.",
		    exception);
	}
    }

}
