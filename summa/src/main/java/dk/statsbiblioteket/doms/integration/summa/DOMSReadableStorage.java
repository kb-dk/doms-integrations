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

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dk.statsbiblioteket.doms.client.DomsWSClient;
import dk.statsbiblioteket.doms.client.DomsWSClientImpl;
import dk.statsbiblioteket.doms.client.exceptions.ServerOperationFailed;
import dk.statsbiblioteket.doms.integration.summa.exceptions.DOMSCommunicationError;
import dk.statsbiblioteket.doms.integration.summa.exceptions.RegistryFullException;
import dk.statsbiblioteket.doms.integration.summa.exceptions.UnknownKeyException;
import dk.statsbiblioteket.doms.integration.summa.parsing.ConfigurationKeys;
import dk.statsbiblioteket.doms.integration.summa.registry.SelfCleaningObjectRegistry;
import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import dk.statsbiblioteket.summa.storage.api.QueryOptions;
import dk.statsbiblioteket.summa.storage.api.Storage;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author Thomas Skou Hansen &lt;tsh@statsbiblioteket.dk&gt;
 */
public class DOMSReadableStorage implements Storage {

    private static final Log log = LogFactory.getLog(DOMSReadableStorage.class);

    /**
     * Three hours in milliseconds. This is the expiration time for the iterator
     * keys returned by this storage.
     */
    private static final long TWELVE_HOURS = 12 * 60 * 60 * 1000;

    /**
     * The delimiter inserted between the Summa base name and the DOMS object
     * UUID when creating the IDs for returned records.
     */
    static final String RECORD_ID_DELIMITER = ":";

    /**
     * By default, allow 100 megabytes per record iterator.
     */
    private static final int DEFAULT_MAX_SIZE_PER_RETRIEVAL = 100*1024*1024;

    /**
     * The client, connected to the DOMS server specified by the configuration,
     * to retrieve objects from.
     */
    private final DomsWSClient domsClient;

    /**
     * <code>Map</code> containing all the configurations for the Summa base
     * names declared in the configuration passed to the
     * <code>DOMSReadableStorage</code> constructor.
     */
    private final Map<String, BaseDOMSConfiguration> baseConfigurations;

    /**
     * A registry containing all record iterators instantiated by methods
     * returning a key associated with an iterator over their result sets.
     */
    private final SelfCleaningObjectRegistry<SummaRecordIterator> recordIterators;
    private final ExecutorService threadPool;

    /**
     * Create a <code>DOMSReadableStorage</code> instance based on the
     * configuration provided by <code>configuration</code>.
     * 
     *
     * @param configuration
     *            Configuration containing information for mapping Summa base
     *            names to DOMS collections and views.
     * @param domsWSClient The doms client to use.
     * @throws ConfigurationException
     *             if the configuration contains any errors regarding option
     *             values or document structure.
     */
    public DOMSReadableStorage(Configuration configuration, DomsWSClient domsWSClient)
            throws ConfigurationException {

        baseConfigurations = new HashMap<>();
        initBaseConfigurations(configuration, baseConfigurations);

        long timeout = configuration.getLong(ConfigurationKeys.ITERATOR_KEY_TIMEOUT, TWELVE_HOURS);
        recordIterators = new SelfCleaningObjectRegistry<>(timeout);

        domsClient = domsWSClient;
        setDomsClientCredentials(configuration);

        threadPool = initialiseThreadPool(configuration.getInt(ConfigurationKeys.DOMS_RETRIEVAL_THREADCOUNT));

    }

    private ExecutorService initialiseThreadPool(Integer viewBundleThreadCount) {
        final ThreadFactory threadFactory = new ThreadFactory() {
            @Override //Hack to make the threads daemon threads so they do not block shutdown
            public Thread newThread(Runnable r) {
                ThreadFactory fac = Executors.defaultThreadFactory();
                Thread thread = fac.newThread(r);
                thread.setDaemon(true);
                return thread;
            }
        };
        //If thread count not correctly specified, make a cached thread pool (creates up to infinity threads as required, and kills them after 60 seconds of idle)
        if (viewBundleThreadCount == null || viewBundleThreadCount <= 0) {
            return Executors.newCachedThreadPool(threadFactory);
        } else {
            return Executors.newFixedThreadPool(viewBundleThreadCount, threadFactory);
        }
    }

    /**
     * Create a <code>DOMSReadableStorage</code> instance based on the
     * configuration provided by <code>configuration</code>.
     *
     * Overloaded constructor to support Summa requirement to constructor.
     * Same as calling {@link #DOMSReadableStorage(dk.statsbiblioteket.summa.common.configuration.Configuration, dk.statsbiblioteket.doms.client.DomsWSClient)}
     * except DomsWSClient is hardcoded to {@link DomsWSClientImpl}.
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
        this(configuration, new DomsWSClientImpl());
    }

    /**
     * Get the time-stamp for when the latest modification occurred in the DOMS
     * collection view identified by <code>base</code>. This method will resolve
     * <code>base</code> to a DOMS collection and view, using the configuration
     * given to this <code>DOMSReadableStorage</code> instance, and query the
     * DOMS for any changes. Please see the interface documentation for further
     * details.
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

            // value...
            if (summaBaseID != null) {

                final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                        .get(summaBaseID);

                final URI collectionPID = baseConfiguration.getCollectionPID();
                final String viewID = baseConfiguration.getViewID();
                final String objectState = baseConfiguration.getObjectState();

                return domsClient.getModificationTime(collectionPID.toString(),
                        viewID, objectState);
            } else {

                long mostRecentTimeStamp = 0;
                for (BaseDOMSConfiguration currentConfiguration : baseConfigurations
                        .values()) {

                    final String collectionPIDString = currentConfiguration
                            .getCollectionPID().toString();
                    final String viewID = currentConfiguration.getViewID();
                    final String objectState = currentConfiguration.getObjectState();

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
        } catch (ServerOperationFailed serverOperationFailed) {
            final String errorMessage = "Failed retrieving the modification time for base: "
                    + summaBaseID;
            log.warn("getModificationTime(): " + errorMessage,
                    serverOperationFailed);
            throw new IOException(errorMessage, serverOperationFailed);
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
            Set<String> iteratorBaseIDs;
            if (summaBaseID != null) {
                iteratorBaseIDs = new LinkedHashSet<>();
                iteratorBaseIDs.add(summaBaseID);
            } else {
                iteratorBaseIDs = baseConfigurations.keySet();
            }

            final SummaRecordIterator recordIterator = new SummaRecordIterator(
                    domsClient, baseConfigurations, iteratorBaseIDs, timeStamp,
                    options, threadPool);

            final long iteratorKey = recordIterators.register(recordIterator);

            if (log.isDebugEnabled()) {
                log.debug("getRecordsModifedAfter(): Returning iteratorKey = "
                        + iteratorKey);
            }
            return iteratorKey;
        } catch (RegistryFullException registryFullException) {
            final String errorMessage = "Failed creating an iterator for "
                    + "retrieval of records from base " + "(base="
                    + summaBaseID + ") modified after time-stamp (timeStamp="
                    + timeStamp + "), using QueryOptions: " + options;
            log.warn("getRecordsModifedAfter(long, String, QueryOptions): "
                    + errorMessage, registryFullException);
            throw new IOException(errorMessage, registryFullException);
        }
    }

    public List<Record> next(long iteratorKey, int maxRecords)
            throws IOException, IllegalArgumentException,
            NoSuchElementException {

        if (log.isTraceEnabled()) {
            log.trace("next(long, int): Called with iteratorKey = "
                    + iteratorKey + " maxRecords = " + maxRecords);
        }

        try {
            final SummaRecordIterator recordIterator = recordIterators
                    .get(iteratorKey);

            if (recordIterator.hasNext() == false) {
                // The iterator has reached the end and thus it is obsolete. Let
                // the wolves have it...
                recordIterators.remove(iteratorKey);

                final String errorMessage = "The iterator is out of records "
                        + "(iterator key = " + iteratorKey + ")";

                log.warn("next(long, int): " + errorMessage);
                throw new NoSuchElementException(errorMessage);
            }

            final long maxSizePerRetrieval = baseConfigurations.get(
                                                                          recordIterator
                                                                                  .getCurrentBaseRecordDescription()
                                                                                  .getSummaBaseID()
                                                                  ).getMaxSizePerRetrieval();
            List<Record> resultList
                    = recordIterator.next(maxRecords, maxSizePerRetrieval);


            if (log.isDebugEnabled()) {
                log.debug("next(long, int): Returning " + resultList.size()
                        + " records.");
            }
            return resultList;
        } catch (UnknownKeyException unknownKeyException) {

            // The iterator key is unknown to the registry. It may be because it
            // has expired.
            final String errorMessage = "Unknown iterator (iterator key = "
                    + iteratorKey + "). Failed retrieving up to " + maxRecords
                    + " records. Has the key expired?";
            log.warn("next(long, int): " + errorMessage);

            throw new IllegalArgumentException(errorMessage,
                    unknownKeyException);
        } catch (DOMSCommunicationError domsCommunicationError) {

            // Translate communication/server errors to IOExceptions!
            final String errorMessage = "next() operation on this iterator "
                    + "(iterator key = " + iteratorKey + ") failed due to a "
                    + "server or communication error. Failed retrieving up to "
                    + maxRecords + " records.";

            log.warn("next(long, int): " + errorMessage);
            throw new IOException(errorMessage, domsCommunicationError);
        }
    }

    public Record next(long iteratorKey) throws IOException,
            IllegalArgumentException, NoSuchElementException {

        if (log.isTraceEnabled()) {
            log.trace("next(long): Called with iteratorKey = " + iteratorKey);
        }

        try {
            final Iterator<Record> recordIterator = recordIterators
                    .get(iteratorKey);

            log.debug("next(long): Returning next element for iterator key: "
                    + iteratorKey);
            return recordIterator.next();
        } catch (NoSuchElementException noSuchElementException) {
            // The iterator has reached the end and thus it is obsolete. Let the
            // wolves have it...
            try {
                recordIterators.remove(iteratorKey);
            } catch (UnknownKeyException unknownKeyException) {
                log.error("next(long): Failed removing iterator from the "
                        + "registry. This is not possible!",
                        unknownKeyException);
                // Just continue although this is probably due to an error
                // somewhere, as the storage will not break because of this.
            }

            if (log.isDebugEnabled()) {
                log.debug("next(long): The iterator is out of records ("
                        + "iterator key = " + iteratorKey + ")");
            }

            // Re-throw.
            throw noSuchElementException;
        } catch (UnknownKeyException unknownKeyException) {

            // The iterator key is unknown to the registry. It may be because it
            // has expired.

            final String errorMessage = "Unknown iterator (iterator key = "
                    + iteratorKey + "). Has the key expired?";
            log.warn("next(long): " + errorMessage);

            throw new IllegalArgumentException(errorMessage,
                    unknownKeyException);
        } catch (DOMSCommunicationError domsCommunicationError) {

            // Translate communication/server errors to IOExceptions!
            final String errorMessage = "next() operation on this iterator "
                    + "(iterator key = " + iteratorKey + ") failed due to a "
                    + "server or communication error.";

            log.warn("next(long): " + errorMessage, domsCommunicationError);
            throw new IOException(errorMessage, domsCommunicationError);
        }
    }

    public Record getRecord(String summaRecordID, QueryOptions options)
            throws IOException, IllegalArgumentException {

        log.trace("getRecord(String , QueryOptions): Called with "
                + "summaRecordID: " + summaRecordID + " QueryOptions: "
                + options);

        // FIXME! Add proper query options handling.

        try {
            // All records previously returned by the DOMS storage have had the
            // base name prepended to the DOMS entry object PID. Thus, we know
            // that there is a base name and a RECPRD_ID_DELIMITER in the
            // beginning of the ID.
            final int baseDelimiterPosition = summaRecordID
                    .indexOf(RECORD_ID_DELIMITER);

            final String base = summaRecordID.substring(0,
                    baseDelimiterPosition);

            final String entryObjectPID = summaRecordID
                    .substring(baseDelimiterPosition + 1);

            final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                    .get(base);

            if (baseConfiguration == null) {

                final String errorMessage = "Unknown Summa base ID: " + base
                        + " in the ID of the requested record: "
                        + summaRecordID;
                log.warn("getRecord(String): " + errorMessage);

                throw new IllegalArgumentException(errorMessage);
            }
            final String viewID = baseConfiguration.getViewID();
            final String viewBundle = domsClient.getViewBundle(entryObjectPID,
                    viewID);

            final Record resultRecord = new Record(summaRecordID, base,
                    viewBundle.getBytes());

            if (log.isDebugEnabled()) {
                log.debug("getRecord(String , QueryOptions): Returning: "
                        + resultRecord);
            }

            return resultRecord;

        } catch (ServerOperationFailed serverOperationFailed) {
            final String errorMessage = "Failed retrieving record (record id = '"
                    + summaRecordID + "', using the query options: " + options;

            log.warn("getRecord(String , QueryOptions): " + errorMessage,
                    serverOperationFailed);
            throw new IOException(errorMessage, serverOperationFailed);
        }
    }

    public List<Record> getRecords(List<String> summaRecordIDs,
            QueryOptions options) throws IOException, IllegalArgumentException {

        if (log.isTraceEnabled()) {
            log.trace("getRecords(List<String>, QueryOptions): called with "
                    + "summaRecordsIDs: " + summaRecordIDs + " QueryOptions: "
                    + options);
        }

        List<Record> resultList = new LinkedList<>();
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
        threadPool.shutdownNow();
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
     *
     *
     * @param configuration
     *            a <code>DOMSReadableStorage</code> configuration object.
     * @throws ConfigurationException
     *             if the configuration contains any illegal values or
     *             structures.
     */
    protected void setDomsClientCredentials(Configuration configuration)
            throws ConfigurationException {

        if (log.isTraceEnabled()) {
            log.trace("setDomsClientCredentials(Configuration): Called with configuration: "
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
            log.warn("setDomsClientCredentials(Configuration): " + errorMessage);

            throw new ConfigurationException(errorMessage);
        }

        final String domsWSEndpointURL = configuration
                .getString(ConfigurationKeys.DOMS_API_WEBSERVICE_URL);
        try {
            final URL domsWSAPIEndpoint = new URL(domsWSEndpointURL);
            domsClient.setCredentials(domsWSAPIEndpoint, userName, password);

            if (log.isDebugEnabled()) {
                log.debug("setDomsClientCredentials(Configuration): returning a DOMSWSClient "
                        + "instance logged in with user = '" + userName
                        + " and password = '" + password + "' on "
                        + domsWSEndpointURL);
            }
        } catch (MalformedURLException malformedURLException) {

            final String errorMessage = "Failed connecting to the DOMS API "
                    + "webservice with the URL" + " (" + domsWSEndpointURL
                    + ") specified in the configuration.";

            log.warn("setDomsClientCredentials(Configuration): " + errorMessage);
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

                final String viewID = subConfiguration
                        .getString(ConfigurationKeys.VIEW_ID);

                final String objectState = subConfiguration
                        .getString(ConfigurationKeys.OBJECT_STATE);

                final int recordCountPerRetrieval = subConfiguration
                        .getInt(ConfigurationKeys.OBJECT_COUNT_PER_RETRIEVAL, 10000);

                final int maxSizePerRetrieval = subConfiguration.getInt(ConfigurationKeys.MAX_SIZE_PER_RETRIEVAL,
                                                                        DEFAULT_MAX_SIZE_PER_RETRIEVAL);

                BaseDOMSConfiguration newBaseConfig = new BaseDOMSConfiguration(
                        collectionPID, viewID, objectState, recordCountPerRetrieval, maxSizePerRetrieval);

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
                log.trace("initBaseConfigurations(Configuration, Map<String, "
                        + "BaseDOMSConfiguration>): " + "Successfully added "
                        + baseConfigurations.size() + " base configurations to"
                        + " the internal map. Returning.");
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

}
