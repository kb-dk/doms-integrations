/*
 * $Id: BaseRecordDescription.java 1069 2010-10-22 13:22:00Z thomassh $
 * $Revision: 1069 $
 * $Date: 2010-10-22 15:22:00 +0200 (Fri, 22 Oct 2010) $
 * $Author: thomassh $
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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dk.statsbiblioteket.doms.centralWebservice.RecordDescription;
import dk.statsbiblioteket.doms.client.DOMSWSClient;
import dk.statsbiblioteket.doms.client.ServerOperationFailed;
import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.storage.api.QueryOptions;

/**
 * @author Thomas Skou Hansen &lt;tsh@statsbiblioteket.dk&gt;
 */
class SummaRecordIterator implements Iterator<Record> {

    private static final Log log = LogFactory.getLog(SummaRecordIterator.class);

    private static final long RECORD_COUNT_PER_RETRIEVAL = 100;

    /**
     * The client, connected to the DOMS server to retrieve objects from.
     */
    private final DOMSWSClient domsClient;

    private final Map<String, BaseDOMSConfiguration> baseConfigurations;
    private final long startTimeStamp;

    // TODO: the Query options will be used some time when we figure out what to
    // expect from it.
    @SuppressWarnings("unused")
    private final QueryOptions queryOptions;

    private final TreeSet<BaseRecordDescription> baseRecordDescriptions;
    private final Map<String, BaseState> baseStates;

    SummaRecordIterator(DOMSWSClient domsClient,
            Map<String, BaseDOMSConfiguration> baseConfigurations,
            Set<String> summaBaseIDs, long timeStamp, QueryOptions options)
            throws InitialisationError {

        this.domsClient = domsClient;
        this.baseConfigurations = baseConfigurations;
        startTimeStamp = timeStamp;
        queryOptions = options;
        baseRecordDescriptions = new TreeSet<BaseRecordDescription>();
        baseStates = createBaseStatesMap(summaBaseIDs);
        try {
            fillCache();
        } catch (ServerOperationFailed serverOperationFailed) {
            throw new InitialisationError(serverOperationFailed);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() {
        if (!baseRecordDescriptions.isEmpty()) {
            return true;
        } else {
            // Attempt filling the cache and return the answer.
            try {
                fillCache();
            } catch (ServerOperationFailed serverOperationFailed) {
                // The DOMS died. Log the event and tell the caller that there
                // are no more records.

                log.warn("hasNext(): Failed initialising the RecordDescription"
                        + " cache.", serverOperationFailed);
            }

            return !baseRecordDescriptions.isEmpty();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        try {
            // Get the next RecordDescription from the cache, build a Record and
            // return it.
            return buildRecord(getNextBaseRecordDescription());
        } catch (ServerOperationFailed serverOperationFailed) {
            throw new NoSuchElementException();
        }
    }

    /**
     * Unsupported operation.
     * 
     * @see java.util.Iterator#remove()
     */
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Get the next BaseRecordDescription from the sorted tree
     * <code>baseRecordDescriptions</code> and update the instance counter for
     * the Summa base which its <code>RecordDescription</code> was retrieved
     * from.
     * 
     * @return the <code>BaseRecordDescription</code> in
     *         <code>baseRecordDescriptions</code> containing the
     *         <code>RecordDescription</code> with the lowest time-stamp.
     * @throws ServerOperationFailed
     *             if the retrieval of <code>RecordDescription</code> instances
     *             from the DOMS fails.
     */
    private BaseRecordDescription getNextBaseRecordDescription()
            throws ServerOperationFailed {

        final BaseRecordDescription baseRecordDescription = baseRecordDescriptions
                .pollFirst();

        final String summaBaseID = baseRecordDescription.getSummaBaseID();
        final BaseState summaBaseState = baseStates.get(summaBaseID);

        final long currentRecordDescriptionCount = summaBaseState
                .getCurrentRecordDescriptionCount() - 1;

        summaBaseState
                .setCurrentRecordDescriptionCount(currentRecordDescriptionCount);

        if (currentRecordDescriptionCount == 0) {
            // Just fetched the last element from this Summa base. Refill...
            fetchBaseRecordDescriptions(summaBaseID);
        }

        return baseRecordDescription;
    }

    /**
     * Create and initialise a <code>BaseState</code> instance for each base ID
     * in <code>baseIDs</code> and associate them in the returned
     * <code>Map</code>.
     * 
     * @param baseIDs
     * @return
     */
    private Map<String, BaseState> createBaseStatesMap(Set<String> baseIDs) {
        Map<String, BaseState> baseStates = new HashMap<String, BaseState>();
        for (String baseID : baseIDs) {
            baseStates.put(baseID, new BaseState());
        }
        return baseStates;
    }

    /**
     * This method fetches a new chunk of <code>RecordDescription</code>
     * instances from the DOMS for each summa base ID present in the
     * <code>baseStates Map</code> having a
     * <code>currentRecordDescriptionCount</code> of zero in its associated
     * <code>BaseState</code>. The size of the chunk of
     * <code>RecordDescription</code> instances is determined by the
     * <code>RECORD_COUNT_PER_RETRIEVAL</code> constant and will be added to the
     * <code>baseRecordDescriptions</code> attribute.
     * 
     * @throws ServerOperationFailed
     *             if any problems are encountered while retriving
     *             <code>RecordDescription</code> instances from the DOMS.
     */
    private void fillCache() throws ServerOperationFailed {

        for (String summaBaseID : baseStates.keySet()) {
            final BaseState summaBaseState = baseStates.get(summaBaseID);
            if (summaBaseState.getCurrentRecordDescriptionCount() == 0) {
                fetchBaseRecordDescriptions(summaBaseID);
            }
        }
    }

    /**
     * Fetch up to <code>recordCountToFetch RecordDescription</code> instances
     * from the DOMS, create <code>BaseRecordDescription</code> for each of them
     * and add them to the <code>baseRecordDescriptions Set</code> attribute.
     * 
     * @param summaBaseID
     * @throws ServerOperationFailed
     */
    private void fetchBaseRecordDescriptions(String summaBaseID)
            throws ServerOperationFailed {

        // Get the configuration for the Summa base ID.
        final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                .get(summaBaseID);

        URI collectionPID = baseConfiguration.getCollectionPID();
        String viewID = baseConfiguration.getViewID();
        final String objectState = "Published";// FIXME! Hard-coded object
        // state!

        List<RecordDescription> retrievedRecordDescriptions = null;
        final BaseState summaBaseState = baseStates.get(summaBaseID);

        final long currentRecordIndex = summaBaseState
                .getNextRecordDescriptionIndex();
        try {
            retrievedRecordDescriptions = domsClient.getModifiedEntryObjects(
                    collectionPID, viewID, startTimeStamp, objectState,
                    currentRecordIndex, RECORD_COUNT_PER_RETRIEVAL);

            // Remove the base information from the base state map if there are
            // no more record descriptions available.
            if (retrievedRecordDescriptions.isEmpty()) {
                baseStates.remove(summaBaseID);
                retrievedRecordDescriptions = new LinkedList<RecordDescription>();
            } else {

                // The current count is supposed to be zero, however, use
                // addition to avoid any errors.
                summaBaseState.setCurrentRecordDescriptionCount(summaBaseState
                        .getCurrentRecordDescriptionCount()
                        + retrievedRecordDescriptions.size());

                // Move the index forward with the amount of records just
                // retrieved.
                summaBaseState.setNextRecordDescriptionIndex(summaBaseState
                        .getNextRecordDescriptionIndex()
                        + retrievedRecordDescriptions.size());
            }

            // Build BaseRecordDescription instances for each RecordDescription
            // retrieved.
            for (RecordDescription recordDescription : retrievedRecordDescriptions) {
                BaseRecordDescription baseRecordDescription = new BaseRecordDescription(
                        summaBaseID, recordDescription);
                baseRecordDescriptions.add(baseRecordDescription);
            }

        } catch (ServerOperationFailed serverOperationFailed) {
            final String errorMessage = "Failed retrieving up to "
                    + RECORD_COUNT_PER_RETRIEVAL + "records " + "(startTime = "
                    + startTimeStamp + " start index = " + currentRecordIndex
                    + " viewID = " + viewID + " objectState = " + objectState
                    + ") from collection (PID = " + collectionPID + ").";

            log.warn("fetchBaseRecordDescriptions(String): " + errorMessage,
                    serverOperationFailed);
            throw serverOperationFailed;
        }
    }

    /**
     * 
     * @param baseRecordDescription
     * @throws NoSuchElementException
     *             if no <code>Record</code> could be built from
     *             </code>baseRecordDescription</code>
     * @return a Summa Storage <code>Record</code> instance built from the
     *         information provided by </code>baseRecordDescription</code>.
     */
    private Record buildRecord(BaseRecordDescription baseRecordDescription) {

        final String summaBaseID = baseRecordDescription.getSummaBaseID();

        final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                .get(summaBaseID);

        try {
            final String viewID = baseConfiguration.getViewID();

            final RecordDescription recordDescription = baseRecordDescription
                    .getRecordDescription();

            final URI modifiedEntryObjectPID = new URI(recordDescription
                    .getPid());

            final byte recordData[] = domsClient.getViewBundle(
                    modifiedEntryObjectPID, viewID).getBytes();

            // Prepend the base name to the PID in order to make it possible
            // for the DOMSReadableStorage.getRecord() methods to figure out
            // what view to use when they are invoked. It's ugly, but hey!
            // That's life....
            final String recordID = summaBaseID
                    + DOMSReadableStorage.RECORD_ID_DELIMITER
                    + modifiedEntryObjectPID.toString();

            final Record newRecord = new Record(recordID, summaBaseID,
                    recordData);

            return newRecord;
        } catch (URISyntaxException uriSyntaxException) {

            final String errorMessage = "Failed retrieving record "
                    + "(startTime = " + startTimeStamp + " viewID = "
                    + baseConfiguration.getViewID()
                    + ") from collection (PID = "
                    + baseConfiguration.getCollectionPID() + ").";
            log.warn("buildRecord(): " + errorMessage, uriSyntaxException);

            throw new NoSuchElementException(errorMessage);
        } catch (ServerOperationFailed serverOperationFailed) {

            final String errorMessage = "Failed retrieving record "
                    + "(startTime = " + startTimeStamp + " viewID = "
                    + baseConfiguration.getViewID()
                    + ") from collection (PID = "
                    + baseConfiguration.getCollectionPID() + ").";
            log.warn("buildRecord(): " + errorMessage, serverOperationFailed);
            throw new NoSuchElementException(errorMessage);
        }
    }
}