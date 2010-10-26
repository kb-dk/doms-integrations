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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import dk.statsbiblioteket.doms.client.DOMSWSClient;
import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import dk.statsbiblioteket.summa.storage.api.QueryOptions;
import dk.statsbiblioteket.summa.storage.api.Storage;

/**
 * @author Thomas Skou Hansen &lt;tsh@statsbiblioteket.dk&gt;
 */
public class DOMSReadableStorage implements Storage {

    private static final Log log = LogFactory.getLog(DOMSReadableStorage.class);

    /**
     * The delimiter inserted between the Summa base name and the DOMS object
     * UUID when creating the IDs for returned records.
     */
    static final String RECORD_ID_DELIMITER = "§";

    /**
     * The client, connected to the DOMS server specified by the configuration,
     * to retrieve objects from.
     */
    private final DOMSWSClient domsClient;

    /**
     * <code>Map</code> containing all the configurations for the Summa base
     * names declared in the configuration passed to the
     * <code>DOMSReadableStorage</code> constructor.
     */
    private final Map<String, BaseDOMSConfiguration> baseConfigurations;

    /**
     * <code>Map</code> containing all record iterators instantiated by methods
     * returning an iterator over their result sets.
     * <p/>
     * FIXME! Currently an iterator and its underlying collection is not
     * deleted/garbage collected if the iterator is not read to the end, that
     * is, until a NoSuchElementException is thrown! Clients do not explicitly
     * indicate when they are done with their iterators which makes it difficult
     * (i.e. impossible) to determine when it is OK to purge them.
     */
    private final Map<Long, Iterator<Record>> recordIterators;

    /**
     * Create a <code>DOMSReadableStorage</code> instance based on the
     * configuration provided by <code>configuration</code>.
     * 
     * @param configuration
     *            Configuration containing information for mapping Summa base
     *            names to DOMS collections and views.
     * @throws ConfigurationException
     *             if the configuration contains any errors regarding option
     *             values or document structure.
     */
    public DOMSReadableStorage(Configuration configuration)
            throws ConfigurationException {

        baseConfigurations = new HashMap<String, BaseDOMSConfiguration>();
        recordIterators = new TreeMap<Long, Iterator<Record>>();

        initBaseConfigurations(configuration, baseConfigurations);
        domsClient = domsLogin(configuration);
    }

    /**
     * Get the time-stamp for when the latest modification occurred in the DOMS
     * collection view identified by <code>base</code>. This method will resolve
     * <code>base</code> to a DOMS collection, content model entry object and
     * view, using the configuration given to this
     * <code>DOMSReadableStorage</code> instance, and query the DOMS for any
     * changes. Please see the interface documentation for further details.
     * 
     * @param summaBaseID
     *            Summa base ID pointing out the DOMS collection and view to
     *            read from..
     * @return The time-stamp in milliseconds for the latest modification made
     *         in the collection identified by <code>base</code>.
     * 
     * @see dk.statsbiblioteket.summa.storage.api.ReadableStorage#getModificationTime(java.lang.String)
     */
    public long getModificationTime(String summaBaseID) throws IOException {

        if (log.isTraceEnabled()) {
            log.trace("DOMSReadableStorage.getModificationTime(): Called with "
                    + "summaBaseID: " + summaBaseID);
        }

        try {
            final String objectState = "Published"; // FIXME! Hard-coded
            // value...
            if (summaBaseID != null) {

                final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                        .get(summaBaseID);

                final URI collectionPID = baseConfiguration.getCollectionPID();
                final String viewID = baseConfiguration.getViewID();

                return domsClient.getModificationTime(collectionPID.toString(),
                        viewID, objectState);
            } else {

                long mostRecentTimeStamp = 0;
                for (BaseDOMSConfiguration currentConfiguration : baseConfigurations
                        .values()) {

                    final String collectionPIDString = currentConfiguration
                            .getCollectionPID().toString();
                    final String viewID = currentConfiguration.getViewID();

                    long currentTimeStamp = domsClient.getModificationTime(
                            collectionPIDString, viewID, objectState);

                    // Update the most recent time-stamp, if necessary.
                    mostRecentTimeStamp = (mostRecentTimeStamp < currentTimeStamp) ? currentTimeStamp
                            : mostRecentTimeStamp;
                }
                if (log.isDebugEnabled()) {
                    log.debug("getModificationTime(): Returning "
                            + "mostRecentTimeStamp: " + mostRecentTimeStamp);
                }
                return mostRecentTimeStamp;
            }
        } catch (Exception exception) {
            final String errorMessage = "Failed retrieving the modification time for base: "
                    + summaBaseID;
            log.warn("getModificationTime(): " + errorMessage, exception);
            throw new IOException(errorMessage, exception);
        }
    }

    public long getRecordsModifiedAfter(long timeStamp, String summaBaseID,
            QueryOptions options) throws IOException {

        if (log.isTraceEnabled()) {
            log.trace("getRecorsModifiedAfter(): Called with timeStamp: "
                    + timeStamp + " summaBaseID: " + summaBaseID
                    + " QueryOptions: " + options);
        }
        // FIXME! Add proper query options handling.

        try {
            Set<String> iteratorBaseIDs = null;
            if (summaBaseID != null) {
                iteratorBaseIDs = new LinkedHashSet<String>();
                iteratorBaseIDs.add(summaBaseID);
            } else {
                iteratorBaseIDs = baseConfigurations.keySet();
            }

            final Iterator<Record> recordIterator = new SummaRecordIterator(
                    domsClient, baseConfigurations, iteratorBaseIDs, timeStamp,
                    options);

            final long iteratorKey = registerIterator(recordIterator);

            if (log.isDebugEnabled()) {
                log.debug("getRecordsModifedAfter(): Returning iteratorKey = "
                        + iteratorKey);
            }
            return iteratorKey;
        } catch (Exception exception) {
            final String errorMessage = "Failed retrieving records from base "
                    + "(base=" + summaBaseID + ") modified after time-stamp "
                    + "(timeStamp=" + timeStamp + "), using QueryOptions: "
                    + options;
            log.warn("getRecordsModifedAfter(): " + errorMessage, exception);
            throw new IOException(errorMessage, exception);
        }
    }

    public List<Record> next(long iteratorKey, int maxRecords)
            throws IOException {

        if (log.isTraceEnabled()) {
            log.trace("next(long, int): Called with iteratorKey = "
                    + iteratorKey + " maxRecords = " + maxRecords);
        }

        final Iterator<Record> recordIterator = recordIterators
                .get(iteratorKey);

        if (recordIterator == null) {
            final String errorMessage = "Unknown record iterator (iterator "
                    + "key: " + iteratorKey + "). Failed retrieving up to "
                    + maxRecords + " records.";

            log.warn("next(long, int): " + errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        if (recordIterator.hasNext() == false) {
            // The iterator has reached the end and thus it is obsolete. Let the
            // wolves have it...
            recordIterators.remove(iteratorKey);

            final String errorMessage = "The iterator is out of records "
                    + "(iterator key = " + iteratorKey + ")";

            log.warn("next(long, int): " + errorMessage);
            throw new NoSuchElementException(errorMessage);
        }

        try {
            final List<Record> resultList = new LinkedList<Record>();
            int recordCounter = 0;
            while (recordIterator.hasNext() && recordCounter < maxRecords) {
                resultList.add(recordIterator.next());
                recordCounter++;
            }

            if (log.isDebugEnabled()) {
                log.debug("next(long, int): Returning " + resultList.size()
                        + " records.");
            }
            return resultList;
        } catch (NoSuchElementException noSuchElementException) {

            // The iterator has reached the end and thus it is obsolete. Let the
            // wolves have it...
            recordIterators.remove(iteratorKey);

            if (log.isDebugEnabled()) {
                log.debug("next(long, int): The iterator is out of records "
                        + "(iterator key = " + iteratorKey + ")");
            }
            // Re-throw.
            throw noSuchElementException;
        }
    }

    public Record next(long iteratorKey) throws IOException {

        if (log.isTraceEnabled()) {
            log.trace("next(long): Called with iteratorKey = " + iteratorKey);
        }

        final Iterator<Record> recordIterator = recordIterators
                .get(iteratorKey);

        if (recordIterator == null) {

            final String errorMessage = "Unknown record iterator (iterator key: "
                    + iteratorKey + "). Failed retrieving a record.";

            log.warn("next(long): " + errorMessage);

            throw new IllegalArgumentException(errorMessage);
        }

        try {
            log.debug("next(long): Returning.");
            return recordIterator.next();
        } catch (NoSuchElementException noSuchElementException) {
            // The iterator has reached the end and thus it is obsolete. Let the
            // wolves have it...
            recordIterators.remove(iteratorKey);

            if (log.isTraceEnabled()) {
                log.trace("The iterator is out of records (iterator key = "
                        + iteratorKey + ")");
            }

            // Re-throw.
            throw noSuchElementException;
        }
    }

    public Record getRecord(String summaRecordID, QueryOptions options)
            throws IOException {

        log.trace("getRecord(String , QueryOptions): Called with "
                + "summaRecordID: " + summaRecordID + " QueryOptions: "
                + options);

        // FIXME! Add proper query options handling.

        try {
            // All records previously returned by the DOMS storage have had the
            // base name prepended to the DOMS entry object PID. Thus, we know
            // that there is a base name and an underscore in the beginning of
            // the ID.
            final int baseDelimiterPosition = summaRecordID
                    .indexOf(RECORD_ID_DELIMITER);

            final String base = summaRecordID.substring(0,
                    baseDelimiterPosition);

            final String contentModelEntryObjectPID = summaRecordID
                    .substring(baseDelimiterPosition + 1);

            final String viewID = baseConfigurations.get(base).getViewID();
            final String viewBundle = domsClient.getViewBundle(
                    contentModelEntryObjectPID, viewID);

            final Record resultRecord = new Record(summaRecordID, base,
                    viewBundle.getBytes());

            if (log.isDebugEnabled()) {
                log.debug("getRecord(String , QueryOptions): Returning: "
                        + resultRecord);
            }

            return resultRecord;

        } catch (Exception exception) {
            final String errorMessage = "Failed retrieving record (record id = '"
                    + summaRecordID + "', using the query options: " + options;

            log.warn("getRecord(String , QueryOptions): " + errorMessage);

            throw new IOException(errorMessage);
        }
    }

    public List<Record> getRecords(List<String> summaRecordIDs,
            QueryOptions options) throws IOException {

        if (log.isTraceEnabled()) {
            log.trace("getRecords(List<String>, QueryOptions): called with "
                    + "summaRecordsIDs: " + summaRecordIDs + " QueryOptions: "
                    + options);
        }

        List<Record> resultList = new LinkedList<Record>();
        for (String recordID : summaRecordIDs) {
            resultList.add(getRecord(recordID, options));
        }

        if (log.isDebugEnabled()) {
            log.debug("getRecords(List<String>, QueryOptions): Returning with "
                    + "resultList: " + resultList);
        }
        return resultList;
    }

    public void flush(Record record, QueryOptions options) throws IOException {
        log.warn("flush(Record, QueryOptions): Un-implemented method called "
                + "with record: " + record + " options: " + options);
        throw new NotImplementedException();
    }

    public void flush(Record record) throws IOException {
        log.warn("flush(Record) Un-implemented method called with record: "
                + record);
        throw new NotImplementedException();
    }

    public void flushAll(List<Record> records, QueryOptions options)
            throws IOException {
        log.warn("flushAll(List, QueryOptions) un-implemented method called "
                + "with records: " + records + " options: " + options);
        throw new NotImplementedException();
    }

    public void flushAll(List<Record> records) throws IOException {
        log.warn("flushAll(List): Un-implemented method called with records: "
                + records);
        throw new NotImplementedException();
    }

    public void close() throws IOException {
        log.warn("close(): Un-implemented method called.");
        throw new NotImplementedException();
    }

    public void clearBase(String base) throws IOException {
        log.warn("clearBase(String): Un-implemented method called with base: "
                + base);
        throw new NotImplementedException();
    }

    public String batchJob(String jobName, String base, long minMtime,
            long maxMtime, QueryOptions options) throws IOException {
        log.warn("batchJob(String, String, long, long, "
                + "QueryOptions) Un-implemented method called with jobName: "
                + jobName + " base: " + base + " minMtime: " + minMtime
                + " maxMtime:" + maxMtime + " options: " + options);
        throw new NotImplementedException();
    }

    /**
     * Create a <code>DOMSReadableStorage</code> instance which is logged into
     * the DOMS server specified in <code>configuration</code> using the
     * username, password and web service end-point also specified by
     * <code>configuration</code>.
     * 
     * @param configuration
     *            a <code>DOMSReadableStorage</code> configuration object.
     * @return a reference to the <code>DOMSReadableStorage</code> instance.
     * @throws ConfigurationException
     *             if the configuration contains any illegal values or
     *             structures.
     */
    private DOMSWSClient domsLogin(Configuration configuration)
            throws ConfigurationException {

        if (log.isTraceEnabled()) {
            log.trace("domsLogin(Configuration): Called with configuration: "
                    + configuration);
        }

        final String userName = configuration
                .getString(ConfigurationKeys.DOMS_USER_NAME);

        final String password = configuration
                .getString(ConfigurationKeys.DOMS_PASSWORD);

        if (userName == null || password == null) {

            final String errorMessage = "Invalid DOMS user credentials in the"
                    + " configuration. username = '" + userName
                    + "'  password = '" + password + "'";
            log.warn("domsLogin(Configuration): " + errorMessage);

            throw new ConfigurationException(errorMessage);
        }

        final String domsWSEndpointURL = configuration
                .getString(ConfigurationKeys.DOMS_API_WEBSERVICE_URL);
        try {
            final DOMSWSClient newDomsClient = new DOMSWSClient();
            final URL domsWSAPIEndpoint = new URL(domsWSEndpointURL);
            newDomsClient.login(domsWSAPIEndpoint, userName, password);

            if (log.isDebugEnabled()) {
                log.debug("domsLogin(Configuration): returning a DOMSWSClient "
                        + "instance logged in with user = '" + userName
                        + " and password = '" + password + "' on "
                        + domsWSEndpointURL);
            }
            return newDomsClient;
        } catch (MalformedURLException malformedURLException) {

            final String errorMessage = "Failed connecting to the DOMS API "
                    + "webservice with the URL" + " (" + domsWSEndpointURL
                    + ") specified in the configuration.";

            log.warn("domsLogin(Configuration): " + errorMessage);
            throw new ConfigurationException(errorMessage,
                    malformedURLException);
        }
    }

    /**
     * Initialise the provided map of base configurations with the relations
     * between the Summa base names and views of DOMS collections as described
     * by the configuration provided by the <code>configuration</code>
     * parameter.
     * <p/>
     * This method will add an entry to the <code>baseConfigurationsMap</code>
     * for each Summa base configuration found in the configuration document.
     * Each of these configurations will be mapped to a base name.
     * 
     * @param configuration
     *            Configuration document describing relations between Summa base
     *            names and DOMS collections, content models and views.
     * 
     * @param baseConfigurationsMap
     *            The map to initialise.
     * @throws ConfigurationException
     *             if the configuration contains any illegal values or document
     *             structures.
     */
    private void initBaseConfigurations(Configuration configuration,
            Map<String, BaseDOMSConfiguration> baseConfigurationsMap)
            throws ConfigurationException {

        if (log.isTraceEnabled()) {
            log.trace("initBaseConfigurations(Configuration, Map<String, "
                    + "BaseDOMSConfiguration>): Called with configuration: "
                    + configuration + " baseConfigurationsMap:"
                    + baseConfigurationsMap);
        }

        String baseID = null;
        BaseDOMSConfiguration previousConfig = null;
        try {
            final List<Configuration> baseConfigurations = configuration
                    .getSubConfigurations(ConfigurationKeys.ACCESSIBLE_COLLECTION_BASES);

            for (Configuration subConfiguration : baseConfigurations) {
                baseID = subConfiguration
                        .getString(ConfigurationKeys.COLLECTION_BASE_ID);

                final String collectionURI = subConfiguration
                        .getString(ConfigurationKeys.COLLECTION_PID);

                final URI collectionPID = new URI(collectionURI);

                final String collectionContentModelURI = subConfiguration
                        .getString(ConfigurationKeys.COLLECTION_ENTRY_CONTENT_MODEL_PID);

                final URI entryContentModelPID = new URI(
                        collectionContentModelURI);

                final String viewID = subConfiguration
                        .getString(ConfigurationKeys.VIEW_ID);

                BaseDOMSConfiguration newBaseConfig = new BaseDOMSConfiguration(
                        collectionPID, entryContentModelPID, viewID);

                previousConfig = baseConfigurationsMap.put(baseID,
                        newBaseConfig);
                if (previousConfig != null) {
                    // We want to throw an exception here, however, there is no
                    // need to get it nested in a fault barrier exception. Thus,
                    // break and throw later.
                    break;
                }
            }

            if (log.isTraceEnabled()) {
                log
                        .trace("initBaseConfigurations(Configuration, Map<String, "
                                + "BaseDOMSConfiguration>): Successfully added "
                                + baseConfigurations.size()
                                + " base configurations to the internal map. Returning.");
            }

        } catch (Exception exception) {

            final String errorMessage = "Could not retrieve the collection base (base ID = '"
                    + baseID + "' configuration information.";

            log.warn("initBaseConfigurations(Configuration, Map<String, "
                    + "BaseDOMSConfiguration>): " + errorMessage);

            throw new ConfigurationException(errorMessage, exception);
        }
        if (previousConfig != null) {
            final String errorMessage = "base (base ID = '" + baseID
                    + "' has already been associated with a" + " DOMS view.";

            log.warn("initBaseConfigurations(Configuration, Map<String, "
                    + "BaseDOMSConfiguration>):" + errorMessage);

            throw new ConfigurationException(errorMessage);
        }
    }

    /**
     * Register <code>iterator</code> in the internal iterator map and return
     * the iterator key.
     * <p/>
     * This method will attempt to generate a random, non-negative iterator key
     * which is not already used by the iterator map. However, if this fails it
     * will throw an exception.
     * 
     * @param iterator
     *            a <code>Record</code> iterator to add to the map.
     * @return the key associated with the iterator.
     * @throws Exception
     *             if no vacant iterator ID could be found.
     */
    private synchronized long registerIterator(Iterator<Record> iterator)
            throws Exception {

        if (log.isTraceEnabled()) {
            log.trace("registerIterator(Iterator<Record>): Called with "
                    + "iterator: " + iterator);
        }

        long iteratorKey = Math.round(Long.MAX_VALUE * Math.random());
        long emergencyBrake = 0;

        while (recordIterators.containsKey(iteratorKey)) {
            iteratorKey = Math.round(Long.MAX_VALUE * Math.random());
            emergencyBrake++;
            if (emergencyBrake == 0) {
                final String errorMessage = "Unable to produce an iterator key.";

                log.warn("registerIterator(Iterator<Record> iteator): "
                        + errorMessage);
                throw new Exception(errorMessage);
            }
        }
        recordIterators.put(iteratorKey, iterator);

        if (log.isTraceEnabled()) {
            log.trace("registerIterator(Iterator<Record> iteator): returning "
                    + "with iteratorKey: " + iteratorKey);
        }

        return iteratorKey;
    }
}
