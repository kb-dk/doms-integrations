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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dk.statsbiblioteket.doms.central.RecordDescription;
import dk.statsbiblioteket.doms.client.DomsWSClient;
import dk.statsbiblioteket.doms.client.exceptions.ServerOperationFailed;
import dk.statsbiblioteket.doms.integration.summa.exceptions.DOMSCommunicationError;
import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.storage.api.QueryOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * This class is an iterator over records from doms. It returns records, retrieved from doms based on
 * BaseRecordDescriptions
 * @author Asger Askov Blekinge
 * @author Thomas Skou Hansen &lt;tsh@statsbiblioteket.dk&gt;
 */
class SummaRecordIterator {

    private static final Log log = LogFactory.getLog(SummaRecordIterator.class);

    /**
     * The client, connected to the DOMS server to retrieve objects from.
     */
    private final DomsWSClient domsClient;

    private final Map<String, BaseDOMSConfiguration> baseConfigurations;
    private final long startTimeStamp;

    // TODO: the Query options will be used some time when we figure out what to
    // expect from it.
    @SuppressWarnings("unused")
    private final QueryOptions queryOptions;
    private final ExecutorService threadPool;

    //TODO: These have to agree on data, this is brittle. Move the baseRecordDescriptions into baseState
    private final PriorityQueue<BaseRecordDescription> baseRecordDescriptions;
    private final Map<String, BaseState> baseStates;

    SummaRecordIterator(DomsWSClient domsClient,
                        Map<String, BaseDOMSConfiguration> baseConfigurations,
                        Set<String> summaBaseIDs, long timeStamp, QueryOptions options,
                        ExecutorService threadPool) {

        this.domsClient = domsClient;
        this.baseConfigurations = baseConfigurations;
        startTimeStamp = timeStamp;
        queryOptions = options;
        this.threadPool = threadPool;
        baseRecordDescriptions = new PriorityQueue<>();
        baseStates = createBaseStatesMap(summaBaseIDs);
    }

    /**
     * @throws DOMSCommunicationError
     *             if the operation fails due to a communication or server
     *             error.
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() throws DOMSCommunicationError {
        if (log.isTraceEnabled()) {
            log.trace("hasNext(): Entering.");
        }

        // Make sure that the cache holds base record descriptions for all
        // active summa base IDs.
        fillCache();

        final boolean hasNextElement = !baseRecordDescriptions.isEmpty();
        if (log.isTraceEnabled()) {
            log.trace("hasNext(): Returning '" + hasNextElement + "'");
        }
        return hasNextElement;
    }

    /**
     * @throws DOMSCommunicationError
     *             if the operation fails due to a communication or server
     *             error.
     * @see java.util.Iterator#next()
     */
    public Record next() {
        final List<Record> next = next(1, Long.MAX_VALUE);
        if (next.isEmpty()){
            throw new NoSuchElementException("Iterator is out of records");
        }
        return next.get(0);
    }


    /**
     * Shorthand for getting a list of records, rather than just one. If the iterator runs out, return an empty list
     *
     * @param maxResults          the maximum length of the result list
     * @param maxSizePerRetrieval the maximum memory usage of the result list
     *
     * @return a list, possible of length 0
     */
    public List<Record> next(int maxResults, long maxSizePerRetrieval) {
        ArrayList<BaseRecordDescription> recordDescriptions = new ArrayList<>();
        for (int i = 0; i < maxResults && hasNext(); i++) {
            final BaseRecordDescription baseRecordDescription = getNextBaseRecordDescription();
            recordDescriptions.add(baseRecordDescription);
        }

        ArrayList<Future<Record>> futureRecordList = new ArrayList<Future<Record>>(recordDescriptions.size());
        for (final BaseRecordDescription recordDescription : recordDescriptions) {
            Future<Record> futureRecord = threadPool.submit(new Callable<Record>() {
                @Override
                public Record call() throws Exception {
                    return buildRecord(recordDescription);
                }
            });
            futureRecordList.add(futureRecord);
        }

        long size = 0;
        ArrayList<Record> resultSet = new ArrayList<>();
        for (int i = 0; i < futureRecordList.size(); i++) {
            Future<Record> recordFuture = futureRecordList.get(i);
            try {
                final Record record = recordFuture.get();
                resultSet.add(record);
                size += record.getContent().length;
                if (size > maxSizePerRetrieval) {
                    for (int j = i + 1; j < futureRecordList.size(); j++) {
                        Future<Record> toCancel = futureRecordList.get(j);
                        toCancel.cancel(true);
                        pushBackBaseRecordDescription(recordDescriptions.get(j));
                    }
                    break;
                }
            } catch (InterruptedException | ExecutionException e) {
                for (BaseRecordDescription recordDescription : recordDescriptions) {
                    pushBackBaseRecordDescription(recordDescription);
                }
                throw new DOMSCommunicationError("Failed to retrieve record from doms", e);
            }
        }
        return resultSet;
    }

       /**
     * Get the next <code>BaseRecordDescription</code> from the sorted tree
     * <code>baseRecordDescriptions</code> and update the instance counter for
     * the Summa base which its <code>RecordDescription</code> was retrieved
     * from.
     *
     * @return the <code>BaseRecordDescription</code> in
     *         <code>baseRecordDescriptions</code> containing the
     *         <code>RecordDescription</code> with the lowest time-stamp.
     */
    private BaseRecordDescription getNextBaseRecordDescription() {

        if (log.isTraceEnabled()) {
            log.trace("getNextBaseRecordDescription(): Entering.");
        }

        final BaseRecordDescription baseRecordDescription = baseRecordDescriptions
                .poll();

        final String summaBaseID = baseRecordDescription.getSummaBaseID();
        final BaseState summaBaseState = baseStates.get(summaBaseID);

        final long currentRecordDescriptionCount = summaBaseState
                .getCurrentRecordDescriptionCount() - 1;

        summaBaseState
                .setCurrentRecordDescriptionCount(currentRecordDescriptionCount);

        if (log.isTraceEnabled()) {
            log.trace("getNextBaseRecordDescription(): Returning "
                    + "BaseRecordDescription: " + baseRecordDescription);
        }
        return baseRecordDescription;
    }

    /**
     * Return (i.e. push back) a <code>BaseRecordDescription</code> to the
     * sorted tree <code>baseRecordDescriptions</code> and update the instance
     * counter for the Summa base which its <code>RecordDescription</code> was
     * retrieved from.
     * <p/>
     *
     * This method enables the iterator to undo a next() operation if it fails
     * to build a <code>Record</code> due to communication/server errors.
     *
     * @param baseRecordDescription
     *            the <code>BaseRecordDescription</code> in
     *            <code>baseRecordDescriptions</code> containing the
     *            <code>RecordDescription</code> with the lowest time-stamp.
     */
    private void pushBackBaseRecordDescription(
            BaseRecordDescription baseRecordDescription) {

        if (log.isTraceEnabled()) {
            log.trace("pushBackBaseRecordDescription(): Entering. "
                    + "baseRecordDescription = " + baseRecordDescription);
        }

        baseRecordDescriptions.add(baseRecordDescription);

        final String summaBaseID = baseRecordDescription.getSummaBaseID();
        BaseState summaBaseState = baseStates.get(summaBaseID);
        if (summaBaseState == null){
            //If it is not in the baseStates map, this base state have been exhausted.
            summaBaseState = new BaseState();
            summaBaseState.setReachedEnd(true);
            //recreate it here, but set reachedEnd, so that it will be collected again
            baseStates.put(summaBaseID, summaBaseState);
        }

        final long currentRecordDescriptionCount = summaBaseState
                .getCurrentRecordDescriptionCount() + 1;

        summaBaseState
                .setCurrentRecordDescriptionCount(currentRecordDescriptionCount);

        if (log.isTraceEnabled()) {
            log.trace("pushBackBaseRecordDescription(): Returning.");
        }
    }

    /**
     * Create and initialise a <code>BaseState</code> instance for each base ID
     * in <code>baseIDs</code> and associate them in the returned
     * <code>Map</code>.
     *
     * @param baseIDs
     *            a <code>Set</code> of base IDs to create base state map from.
     * @return a <code>Map</code> which associates each of the base IDs from
     *         <code>baseIDs</code> with a <code>BaseState</code> instance.
     */
    private Map<String, BaseState> createBaseStatesMap(Set<String> baseIDs) {
        if (log.isTraceEnabled()) {
            log.trace("createBaseStatesMap(Set<String>): Entering.");
        }
        Map<String, BaseState> baseStates = new HashMap<>();
        for (String baseID : baseIDs) {
            baseStates.put(baseID, new BaseState());
        }

        if (log.isTraceEnabled()) {
            log.trace("createBaseStatesMap(Set<String>): Returning a map with "
                    + baseStates.size() + " base state associations.");
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
     * @throws DOMSCommunicationError
     *             if any problems are encountered while retriving
     *             <code>RecordDescription</code> instances from the DOMS.
     */
    private void fillCache() throws DOMSCommunicationError {

        if (log.isTraceEnabled()) {
            log.trace("fillCache(): Entering.");
        }
        boolean updates = false;
        for (Iterator<String> iterator = baseStates.keySet().iterator(); iterator.hasNext(); ) {
            String summaBaseID = iterator.next();
            final BaseState summaBaseState = baseStates.get(summaBaseID);
            if (summaBaseState.getCurrentRecordDescriptionCount() == 0) {
                fetchBaseRecordDescriptions(summaBaseID, iterator);
                updates = true;
            }
        }

        if (updates){
            log.info("fillCache(): Successfully updated the cache for " + baseStates.keySet().size() + " base IDs.");
        }
    }

    /**
     * Fetch up to <code>recordCountToFetch RecordDescription</code> instances
     * from the DOMS, create <code>BaseRecordDescription</code> for each of them
     * and add them to the <code>baseRecordDescriptions Set</code> attribute.
     *
     *
     * @param summaBaseID
     *            the ID to use for resolving the collection PID and view ID to
     *            use when building the <code>BaseRecordDescription</code>
     *            instances.
     * @param iterator The iterator currently going through base collections, used for removing.
     * @throws DOMSCommunicationError
     *             if the operation fails due to a communication or server
     *             error.
     */
    private void fetchBaseRecordDescriptions(String summaBaseID, Iterator<String> iterator)
            throws DOMSCommunicationError {

        if (log.isTraceEnabled()) {
            log.trace("fetchBaseRecordDescriptions(String): Entering. "
                    + "summaBaseID = " + summaBaseID);
        }
        // Get the configuration for the Summa base ID.
        final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                .get(summaBaseID);

        final String collectionPIDString = baseConfiguration.getCollectionPID()
                .toString();
        String viewID = baseConfiguration.getViewID();

        final String objectState = baseConfiguration.getObjectState();

        List<RecordDescription> retrievedRecordDescriptions = new LinkedList<>();
        final BaseState summaBaseState = baseStates.get(summaBaseID);

        long startTime = summaBaseState.getNextStartTime();
        if (startTime < 0){
            startTime = this.startTimeStamp;
        }
        if (!summaBaseState.isReachedEnd()){


            retrievedRecordDescriptions = retrieveRecordDescriptions(
                    collectionPIDString, viewID, objectState, startTime,
                    baseConfiguration.getRecordCountPerRetrieval());
        } else {
            log.info("Iterator for basestate "+summaBaseID+" was marked as having reached end previously. Do not fetch" +
                    " any further objects");
        }
        // Remove the base information from the base state map if there are
        // no more record descriptions available.
        if (retrievedRecordDescriptions.isEmpty()) {
            iterator.remove();


            log.info("fetchBaseRecordDescriptions(String): The DOMS "
                    + "has no more records for this base (base ID = '"
                    + summaBaseID + "'. Removing it from the map of "
                    + "active base IDs.");

        }  else {
            if (retrievedRecordDescriptions.size() < baseConfiguration.getRecordCountPerRetrieval()){
                summaBaseState.setReachedEnd(true);
            }

            final long currentCount = summaBaseState
                    .getCurrentRecordDescriptionCount();
            if (currentCount != 0) {
                log.debug("fetchBaseRecordDescriptions(String): The cache" + " size for this base ID (" + summaBaseID
                                  + ") was non-zero (actual size = " + currentCount
                                  + ") when this re-fill was requested.");
            }
            // The current count is supposed to be zero, however, use
            // addition to avoid any errors.
            summaBaseState.setCurrentRecordDescriptionCount(currentCount
                    + retrievedRecordDescriptions.size());
            summaBaseState.setNextStartTime(retrievedRecordDescriptions.get(retrievedRecordDescriptions.size()-1).getDate());

        }

        // Build BaseRecordDescription instances for each RecordDescription
        // retrieved.
        for (RecordDescription recordDescription : retrievedRecordDescriptions) {
            final BaseRecordDescription baseRecordDescription = new BaseRecordDescription(
                    summaBaseID, recordDescription);
            baseRecordDescriptions.add(baseRecordDescription);
        }
        log.info("fetchBaseRecordDescriptions(String): Returning "
                + "after adding " + retrievedRecordDescriptions.size()
                + " record descriptions to the cache for the Summa"
                + " base ID: " + summaBaseID);

    }

    /**
     * Retrieve a chunk of <code>RecordDescriptions</code> from the DOMS for all
     * objects that have been modified or have modified objects associated in
     * the specified view. The size of the chunk is specified by the constant
     * <code>RECORD_COUNT_PER_RETRIEVAL</code>.
     *
     * @param collectionPIDString
     *            The PID of the collection to retrieve
     *            <code>RecordDescription</code> instances from.
     * @param viewID
     *            ID of the view to use when checking for modifications.
     * @param objectState
     *            The state an object must be in, in order to be a candidate for
     *            retrieval.
     * @param startTimeStamp
     *            The timestamp to Start retrieval from
     * @param recordCountPerRetrieval
     *            The number of records to retrieve in one retrieval.
     * @return a <code>List</code> of
     *         <code>RecordDescription<code> instances identifying DOMS objects
     *          which have been modified.
     * @throws DOMSCommunicationError
     *             if the retrieval fails due to a communication or server
     *             error.
     */
    private List<RecordDescription> retrieveRecordDescriptions(
            String collectionPIDString, String viewID, String objectState,
            long startTimeStamp, long recordCountPerRetrieval) throws DOMSCommunicationError {

        try {
            return domsClient.getModifiedEntryObjects(collectionPIDString,
                    viewID, startTimeStamp, objectState, 0, recordCountPerRetrieval);
        } catch (ServerOperationFailed serverOperationFailed) {
            final String errorMessage = "Failed retrieving up to "
                    + recordCountPerRetrieval + "records " + "(startTime = "
                    + startTimeStamp + " start index = " + 0
                    + " viewID = " + viewID + " objectState = " + objectState
                    + ") from the specified collection (PID = "
                    + collectionPIDString + ").";

            log.warn("retrieveRecordDescriptions(String, String, "
                    + "String, long): " + errorMessage, serverOperationFailed);

            // Give up... Let the fault barrier handle this.
            throw new DOMSCommunicationError(errorMessage,
                    serverOperationFailed);
        }
    }

    /**
     * Build a Summa <code>Record</code> from the information provided by the
     * <code>BaseRecordDescription</code> provided by
     * <code>baseRecordDescription</code>.
     *
     *
     * @param baseRecordDescription
     *            a <code>BaseRecordDescription</code> instance containing the
     *            necessary information for building a <code>Record</code>.
     * @throws ServerOperationFailed
     *             if no <code>Record</code> could be built due to a
     *             communication or DOMS server error.
     * @return a Summa Storage <code>Record</code> instance built from the
     *         information provided by </code>baseRecordDescription</code>.
     */
    private Record buildRecord(BaseRecordDescription baseRecordDescription)
            throws ServerOperationFailed {

        if (log.isTraceEnabled()) {
            log.trace("buildRecord(BaseRecordDescription): Entering. "
                    + "baseRecordDescription = " + baseRecordDescription);
        }

        final String summaBaseID = baseRecordDescription.getSummaBaseID();

        final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                .get(summaBaseID);

        try {
            final String viewID = baseConfiguration.getViewID();

            final RecordDescription recordDescription = baseRecordDescription
                    .getRecordDescription();

            final String modifiedEntryObjectPIDString = recordDescription
                    .getPid();

            // Prepend the base name to the PID in order to make it possible
            // for the DOMSReadableStorage.getRecord() methods to figure out
            // what view to use when they are invoked. It's ugly, but hey!
            // That's life....
            final String recordID = summaBaseID + DOMSReadableStorage.RECORD_ID_DELIMITER +
                                    modifiedEntryObjectPIDString;
            final Record newRecord;
            if (recordDescription.getState().equalsIgnoreCase("D")){
                newRecord = new Record(recordID, summaBaseID, new byte[0]);
                newRecord.setDeleted(true);
            } else {
                final byte recordData[] = domsClient.getViewBundle(modifiedEntryObjectPIDString, viewID).getBytes();
                newRecord = new Record(recordID, summaBaseID, recordData);
            }
            newRecord.setModificationTime(recordDescription.getDate());
            newRecord.setCreationTime(recordDescription.getDate());

            if (log.isTraceEnabled()) {
                log.trace("buildRecord(BaseRecordDescription): Returning a "
                        + "record (recordID = '" + recordID
                        + "' summaBaseID = '" + summaBaseID + "')");
            }
            return newRecord;
        } catch (ServerOperationFailed serverOperationFailed) {

            final String errorMessage = "Failed retrieving record "
                    + "(viewID = "
                    + baseConfiguration.getViewID()
                    + ") from collection (PID = "
                    + baseConfiguration.getCollectionPID() + ").";
            log.warn("buildRecord(): " + errorMessage, serverOperationFailed);
            throw serverOperationFailed;
        }
    }

    BaseRecordDescription getCurrentBaseRecordDescription() {
        return baseRecordDescriptions.peek();
    }

}
