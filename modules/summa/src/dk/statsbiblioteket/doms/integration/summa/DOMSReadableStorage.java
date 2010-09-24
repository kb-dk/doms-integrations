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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import dk.statsbiblioteket.doms.centralWebservice.RecordDescription;
import dk.statsbiblioteket.doms.centralWebservice.TrackerRecord;
import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import dk.statsbiblioteket.summa.storage.api.QueryOptions;
import dk.statsbiblioteket.summa.storage.api.ReadableStorage;

/**
 * @author &lt;tsh@statsbiblioteket.dk&gt;
 * 
 */
public class DOMSReadableStorage implements ReadableStorage {

    private final Configuration configuration;
    private final DOMSWSClient domsClient;
    private final Map<String, URI> baseCollectionPIDMap;
    private final Map<String, URI> baseEntryContentModelPIDMap;
    private final Map<String, String> baseViewIDMap;

    private final Map<Long, Iterator<Record>> recordIterators;

    /**
     * 
     * @param configuration
     * @throws ConfigurationException
     */
    public DOMSReadableStorage(Configuration configuration)
	    throws ConfigurationException {
	this.configuration = configuration;
	baseCollectionPIDMap = new HashMap<String, URI>();
	baseEntryContentModelPIDMap = new HashMap<String, URI>();
	baseViewIDMap = new HashMap<String, String>();
	recordIterators = new TreeMap<Long, Iterator<Record>>();

	initIDMaps(configuration, baseCollectionPIDMap,
	        baseEntryContentModelPIDMap, baseViewIDMap);
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
	    final URI collectionPID = baseCollectionPIDMap.get(base);
	    final String viewID = baseViewIDMap.get(base);
	    final URI entryContentModelPID = baseEntryContentModelPIDMap
		    .get(base);
	    return domsClient.getModificationTime(collectionPID, viewID,
		    entryContentModelPID);
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
    public long getRecordsModifiedAfter(long timeStamp, String base,
	    QueryOptions options) throws IOException {

	try {
	    final URI collectionPID = baseCollectionPIDMap.get(base);
	    final String viewID = baseViewIDMap.get(base);
	    final URI entryContentModelPID = baseEntryContentModelPIDMap
		    .get(base);

	    // Remember!!! TrackerRecord.getPid() = PID of the entry object

	    List<RecordDescription> recordDescriptions = domsClient
		    .getModifiedEntryObjects(collectionPID, viewID,
		            entryContentModelPID, timeStamp, "Published");// FIXME!
									  // Hard-coded
									  // state.
									  // What
									  // about
									  // an
									  // enum?

	    // FIXME! Clarify how QueryOptions should be handled and implement
	    // filter-magic here...

	    // Trivial first-shot record and iterator construction.
	    final ArrayList<Record> modifiedRecords = new ArrayList<Record>();
	    for (RecordDescription recordDescription : recordDescriptions) {
		final URI modifiedEntryObjectPID = new URI(recordDescription
		        .getPid());
		final byte data[] = domsClient.getViewBundle(
		        modifiedEntryObjectPID, viewID).getBytes();
		final Record newRecord = new Record(modifiedEntryObjectPID
		        .toString(), base, data);
		modifiedRecords.add(newRecord);
	    }

	    return registerIterator(modifiedRecords.iterator());
	} catch (Exception exception) {
	    throw new IOException("Failed retrieving records from base (base="
		    + base + ") modified after time-stamp (timeStamp="
		    + timeStamp + "), using QueryOptions: " + options,
		    exception);
	}
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
	    Map<String, URI> baseToCollectionPIDMap,
	    Map<String, URI> baseToEntryContentModelPIDMap,
	    Map<String, String> baseToViewIDMap) throws ConfigurationException {
	String baseID = null;
	try {
	    final List<Configuration> baseConfigurations = configuration
		    .getSubConfigurations(ConfigurationKeys.ACCESSIBLE_COLLECTION_BASES);

	    for (Configuration subConfiguration : baseConfigurations) {
		baseID = subConfiguration
		        .getString(ConfigurationKeys.COLLECTION_BASE_ID);

		final String collectionID_URI = subConfiguration
		        .getString(ConfigurationKeys.COLLECTION_ID);
		baseToCollectionPIDMap.put(baseID, new URI(collectionID_URI));

		final String collectionContentModelURI = subConfiguration
		        .getString(ConfigurationKeys.COLLECTION_ENTRY_CONTENT_MODEL_ID);
		baseToEntryContentModelPIDMap.put(baseID, new URI(
		        collectionContentModelURI));

		final String baseViewID = subConfiguration
		        .getString(ConfigurationKeys.BASE_VIEW_ID);
		baseToViewIDMap.put(baseID, baseViewID);
	    }
	} catch (Exception exception) {
	    throw new ConfigurationException(
		    "Could not retrieve the collection base (base ID = '"
		            + baseID + "' configuration information.",
		    exception);
	}
    }

    /**
     * Register <code>iterator</code> in the internal iterator map and return
     * the iterator key.
     * 
     * @param iterator
     * @return
     */
    private synchronized long registerIterator(Iterator<Record> iterator)
	    throws Exception {
	long iteratorKey = Math.round(Long.MAX_VALUE * Math.random());
	long emergencyBrake = 0;
	while (recordIterators.containsKey(iteratorKey)) {
	    iteratorKey = Math.round(Long.MAX_VALUE * Math.random());
	    emergencyBrake++;
	    if (emergencyBrake == 0) {
		throw new Exception("Unable to produce an iterator key.");
	    }
	}
	// FIXME! Watch out! Currently, nobody removes the iterators again!
	recordIterators.put(iteratorKey, iterator);
	return iteratorKey;
    }
}
