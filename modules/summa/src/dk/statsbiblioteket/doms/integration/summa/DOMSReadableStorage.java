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
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import dk.statsbiblioteket.doms.centralWebservice.RecordDescription;
import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import dk.statsbiblioteket.summa.storage.api.QueryOptions;
import dk.statsbiblioteket.summa.storage.api.Storage;
import dk.statsbiblioteket.util.Logs;


/**
 * @author &lt;tsh@statsbiblioteket.dk&gt;
 *
 */
public class DOMSReadableStorage implements Storage {

    private static final Log log = LogFactory.getLog(DOMSReadableStorage.class);

    /**
     * The delimiter inserted between the Summa base name and the DOMS object
     * UUID when creating the IDs for returned records.
     */
    private static final String RECORD_ID_DELIMITER = "§";

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
        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.getModificationTime(): Called with "
                        + "summaBaseID: ", summaBaseID);
        try {
            if (summaBaseID != null) {

                final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                        .get(summaBaseID);
                final URI collectionPID = baseConfiguration.getCollectionPID();
                final String viewID = baseConfiguration.getViewID();
                final URI contentModelEntryObjectPID = baseConfiguration
                        .getContentModelEntryObjectPID();

                return domsClient.getModificationTime(collectionPID, viewID,
                        contentModelEntryObjectPID);
            } else {

                long mostRecentTimeStamp = 0;
                for (BaseDOMSConfiguration currentConfiguration : baseConfigurations
                        .values()) {
                    final URI collectionPID = currentConfiguration
                            .getCollectionPID();
                    final String viewID = currentConfiguration.getViewID();
                    final URI contentModelEntryObjectPID = currentConfiguration
                            .getContentModelEntryObjectPID();

                    long currentTimeStamp = domsClient.getModificationTime(
                            collectionPID, viewID, contentModelEntryObjectPID);

                    // Update the most recent time-stamp, if necessary.
                    mostRecentTimeStamp = (mostRecentTimeStamp < currentTimeStamp) ? currentTimeStamp
                            : mostRecentTimeStamp;
                }
                Logs.log(log, Logs.Level.TRACE,
                        "DOMSReadableStorage.getModificationTime(): returning"
                                + " mostRecentTimeStamp: ", mostRecentTimeStamp);
                return mostRecentTimeStamp;
            }
        } catch (Exception exception) {
            Logs.log(log, Logs.Level.WARN,
                    "Failed retrieving the modification time for base: ",
                    summaBaseID, exception);
            throw new IOException(
                    "Failed retrieving the modification time for base: "
                            + summaBaseID, exception);
        }
    }

    public long getRecordsModifiedAfter(long timeStamp, String summaBaseID,
                                        QueryOptions options) throws IOException {
        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.getRecorsModifiedAfter(): Called with "
                        + "timeStamp: ", timeStamp, " summaBaseID: ", summaBaseID,
                        " QueryOptions: ", options);
        // FIXME! Add proper query options handling.

        try {
            List<Record> resultRecords = null;
            if (summaBaseID != null) {
                resultRecords = getSingleBaseRecordsModifiedAfter(timeStamp,
                        summaBaseID, options);
            } else {
                resultRecords = new LinkedList<Record>();
                for (String currentBaseID : baseConfigurations.keySet()) {
                    resultRecords.addAll(getSingleBaseRecordsModifiedAfter(
                            timeStamp, currentBaseID, options));
                }
            }
            Logs.log(log, Logs.Level.TRACE,
                    "DOMSReadableStorage.getRecordsModifedAfter(): returning"
                            + " registerIterator(resultRecords.iterator())");
            return registerIterator(resultRecords.iterator());
        } catch (Exception exception) {
            Logs.log(log, Logs.Level.WARN,
                    "Failed retrieving records time from base: ", summaBaseID,
                    " modified after timestamp: ",timeStamp, "using "
                            + "QueryOptions: ", options, exception);
            throw new IOException("Failed retrieving records from base (base="
                    + summaBaseID + ") modified after time-stamp (timeStamp="
                    + timeStamp + "), using QueryOptions: " + options,
                    exception);
        }
    }

    public List<Record> next(long iteratorKey, int maxRecords)
            throws IOException {
        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.next(long, int): Called with iteratorKey:"
                , iteratorKey, " maxRecords: ", maxRecords);

        final Iterator<Record> recordIterator = recordIterators
                .get(iteratorKey);

        if (recordIterator == null) {
            Logs.log(log, Logs.Level.WARN,
                    "Unknown record iterator (iterator key: ", iteratorKey,
                    "). Failed retrieving up to ", maxRecords, " records.");
            throw new IllegalArgumentException(
                    "Unknown record iterator (iterator key: " + iteratorKey
                            + "). Failed retrieving up to " + maxRecords
                            + " records.");
        }

        if (recordIterator.hasNext() == false) {
            // The iterator has reached the end and thus it is obsolete. Let the
            // wolves have it...
            recordIterators.remove(iteratorKey);

            // TODO: It may be a good idea to log the iterator key of the
            // iterator that ran out of elements.
            Logs.log(log, Logs.Level.WARN,
                    "The iterator is out of records (iterator key = ",
                    iteratorKey, ")");
            throw new NoSuchElementException(
                    "The iterator is out of records (iterator key = "
                            + iteratorKey + ")");
        }

        try {
            final List<Record> resultList = new LinkedList<Record>();
            int recordCounter = 0;
            while (recordIterator.hasNext() && recordCounter < maxRecords) {
                resultList.add(recordIterator.next());
                recordCounter++;
            }
            Logs.log(log, Logs.Level.TRACE,
                    "DOMSReadableStorage.next(long, int): returning"
                    + "resultList", resultList);
            return resultList;
        } catch (NoSuchElementException noSuchElementException) {
            // The iterator has reached the end and thus it is obsolete. Let the
            // wolves have it...
            recordIterators.remove(iteratorKey);

            // TODO: It may be a good idea to log the iterator key of the
            // iterator that ran out of elements.
            Logs.log(log, Logs.Level.WARN,
                    "The iterator is out of records (iterator key = ",
                    iteratorKey, ")");
            // Re-throw.
            throw noSuchElementException;
        }
    }

    public Record next(long iteratorKey) throws IOException {

        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.next(long): Called with iteratorKey:",
                iteratorKey);

        final Iterator<Record> recordIterator = recordIterators
                .get(iteratorKey);

        if (recordIterator == null) {
            Logs.log(log, Logs.Level.WARN,
                    "Unknown record iterator (iterator key: ", iteratorKey,
                    "). Failed retrieving a record.");

            throw new IllegalArgumentException(
                    "Unknown record iterator (iterator key: " + iteratorKey
                            + "). Failed retrieving a record.");
        }

        try {
            Logs.log(log, Logs.Level.TRACE,
                    "DOMSReadableStorage.next(long): returning");            
            return recordIterator.next();
        } catch (NoSuchElementException noSuchElementException) {
            // The iterator has reached the end and thus it is obsolete. Let the
            // wolves have it...
            recordIterators.remove(iteratorKey);

            // TODO: It may be a good idea to log the iterator key of the
            // iterator that ran out of elements.
            Logs.log(log, Logs.Level.WARN,
                    "The iterator is out of records (iterator key = ",
                    iteratorKey, ")");
            // Re-throw.
            throw noSuchElementException;
        }
    }

    public Record getRecord(String summaRecordID, QueryOptions options)
            throws IOException {

        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.getRecord(): Called with summaRecordID: ",
                summaRecordID, " QueryOptions: ", options);

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
            final String viewBundle = domsClient.getViewBundle(new URI(
                    contentModelEntryObjectPID), viewID);
            Logs.log(log, Logs.Level.TRACE,
                    "DOMSReadableStorage.getRecord(): returning: "
                    + "new Record(summaRecordID, base, viewBundle.getBytes())");
            return new Record(summaRecordID, base, viewBundle.getBytes());
        } catch (Exception exception) {
            Logs.log(log, Logs.Level.WARN,
                    "Failed retrieving record (record id ='", summaRecordID,
                    "', using the query options: ", options);
            throw new IOException("Failed retrieving record (record id = '"
                    + summaRecordID + "', using the query options: " + options);
        }
    }

    public List<Record> getRecords(List<String> summaRecordIDs,
                                   QueryOptions options) throws IOException {
        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.getRecords() called with summaRecordsIDs"
                +"s:",summaRecordIDs," QueryOptions: ", options);

        List<Record> resultList = new LinkedList<Record>();
        for (String recordID : summaRecordIDs) {
            resultList.add(getRecord(recordID, options));
        }
        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.getRecords() returning with "
                +"resultList: ",resultList);
        return resultList;
    }

    public void flush(Record record, QueryOptions options) throws IOException {
        Logs.log(log, Logs.Level.WARN, "flush(Record, QueryOptions)"
                + " unimplemented method called with record: ", record,
                " options: ", options);
        throw new NotImplementedException();
    }

    public void flush(Record record) throws IOException {
        Logs.log(log, Logs.Level.WARN, "flush(Record) unimplemented "
                + "method called with record: ", record);
        throw new NotImplementedException();
    }

    public void flushAll(List<Record> records, QueryOptions options)
            throws IOException {
        Logs.log(log, Logs.Level.WARN, "flushAll(List, QueryOptions) "
                + "unimplemented method called with records: ", records,
                " options: ", options);
        throw new NotImplementedException();
    }

    public void flushAll(List<Record> records) throws IOException {
        Logs.log(log, Logs.Level.WARN, "flushAll(List) unimplemented method"
                + " called with records: ", records);
        throw new NotImplementedException();
    }

    public void close() throws IOException {
        Logs.log(log, Logs.Level.WARN, "close() unimplemented method called");
        throw new NotImplementedException();
    }

    public void clearBase(String base) throws IOException {
        Logs.log(log, Logs.Level.WARN, "clearBase(String) unimplemented "
                + "method called with base: ", base);
        throw new NotImplementedException();
    }

    public String batchJob(String jobName, String base, long minMtime,
                           long maxMtime, QueryOptions options) throws IOException {
        Logs.log(log, Logs.Level.WARN, "batchJob(String, String, long, long, "
                + "QueryOptions) unimplemented method called with jobName: ",
                jobName, " base: ", base, " minMtime: ", minMtime, " maxMtime:"
                , maxMtime, " options: ", options);
        throw new NotImplementedException();
    }

    /**
     * Get all the records modified later than the given time-stamp, for a given
     * Summa base.
     *
     * @param timeStamp
     *            the time-stamp after which the modified records must be
     *            selected.
     * @param summaBaseID
     *            the base to look for modifications in.
     * @param options
     *            <code>QueryOptions</code> containing further selection
     *            criteria specified by the caller.
     * @return a list of records modified after <code>timeStamp</code> and
     *         matching the specified <code>options</code>.
     * @throws ServerOperationFailed
     *             if any errors are encountered when communicating with the
     *             DOMS.
     * @throws URISyntaxException
     *             if a PID, returned by the DOMS, is an invalid
     *             <code>URI</code>. This is quite unlikely to happen.
     */
    private List<Record> getSingleBaseRecordsModifiedAfter(long timeStamp,
                                                           String summaBaseID, QueryOptions options) throws ServerOperationFailed,
            URISyntaxException {
        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.getSingleBaseRecordsModifiedAfter()"
                + "called with timestamp: ", timeStamp, " summaBaseID: ",
                summaBaseID, " QueryOptions: ", options);

        final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                .get(summaBaseID);

        final URI collectionPID = baseConfiguration.getCollectionPID();
        final String viewID = baseConfiguration.getViewID();
        final URI contentModelEntryObjectPID = baseConfiguration
                .getContentModelEntryObjectPID();

        List<RecordDescription> recordDescriptions = domsClient
                .getModifiedEntryObjects(collectionPID, viewID,
                        contentModelEntryObjectPID, timeStamp, "Published");
        // FIXME! Hard-coded "Published" state. What about an enum?

        // FIXME! Clarify how QueryOptions should be handled and
        // implement filter-magic here...

        // Trivial first-shot record and iterator construction.
        final List<Record> modifiedRecords = new LinkedList<Record>();
        for (RecordDescription recordDescription : recordDescriptions) {

            // Get the PID of the modified content model entry object.
            final URI modifiedEntryCMObjectPID = new URI(recordDescription
                    .getPid());
            final byte data[] = domsClient.getViewBundle(
                    modifiedEntryCMObjectPID, viewID).getBytes();

            // Prepend the base name to the PID in order to make it possible
            // for the getRecord() methods to figure out what view to use
            // when they are invoked. It's ugly, but hey! That's life....
            final String summaRecordID = summaBaseID + RECORD_ID_DELIMITER
                    + modifiedEntryCMObjectPID.toString();
            final Record newRecord = new Record(summaRecordID, summaBaseID,
                    data);
            modifiedRecords.add(newRecord);
        }
        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.getSingleBaseRecordsModifiedAfter()"
                + "returning with modifiedRecorsd", modifiedRecords);
        return modifiedRecords;
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

        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.domsLogin() called with configuration: ",
                configuration);

        final String userName = configuration
                .getString(ConfigurationKeys.DOMS_USER_NAME);

        final String password = configuration
                .getString(ConfigurationKeys.DOMS_PASSWORD);

        if (userName == null || password == null) {
            Logs.log(log, Logs.Level.WARN,
                    "DOMSReadableStorage.domsLogin(): Invalid DOMS user "
                    + "credentials in the configuration. username = ",
                    userName, " password = ", password);
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
            Logs.log(log, Logs.Level.TRACE,
                    "DOMSReadableStorage.domsLogin() returning with "
                    + "newDomsClient: ", newDomsClient);
            return newDomsClient;
        } catch (MalformedURLException malformedURLException) {
            Logs.log(log, Logs.Level.WARN,
                    "DOMSReadableStorage.domsLogin(): Failed connecting to "
                    + "the DOMS API webservice with the URL: ",
                    domsWSEndpointURL, " specified in the configuration.");
            throw new ConfigurationException(
                    "Failed connecting to the DOMS API webservice with the URL"
                            + " (" + domsWSEndpointURL
                            + ") specified in the configuration.",
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

        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.initBaseConfigurations() called with "
                + "configuration: ", configuration, " baseConfigurationsMap:",
                baseConfigurationsMap);

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
        } catch (Exception exception) {
            Logs.log(log, Logs.Level.WARN,
                    "Could not retrieve the collection base (baseID: ",
                    baseID, " configuration information.");
            throw new ConfigurationException(
                    "Could not retrieve the collection base (base ID = '"
                            + baseID + "' configuration information.",
                    exception);
        }
        if (previousConfig != null) {
            Logs.log(log, Logs.Level.WARN,
                    "baseID: ", baseID, " has already been associated with"
                    + " a DOMS view.");
            throw new ConfigurationException("base (base ID = '" + baseID
                    + "' has already been associated with a"
                    + " DOMS view.");
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
        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.registerIterator() called with iterator",
                iterator);
        long iteratorKey = Math.round(Long.MAX_VALUE * Math.random());
        long emergencyBrake = 0;

        while (recordIterators.containsKey(iteratorKey)) {
            iteratorKey = Math.round(Long.MAX_VALUE * Math.random());
            emergencyBrake++;
            if (emergencyBrake == 0) {
                Logs.log(log, Logs.Level.WARN,
                        "Unable to produce an iterator key.");
                throw new Exception("Unable to produce an iterator key.");
            }
        }
        recordIterators.put(iteratorKey, iterator);
        Logs.log(log, Logs.Level.TRACE,
                "DOMSReadableStorage.registerIterator(Iterator<Record>"
                + " iteator) returning with iteratorKey:", iteratorKey);
        return iteratorKey;
    }
}
