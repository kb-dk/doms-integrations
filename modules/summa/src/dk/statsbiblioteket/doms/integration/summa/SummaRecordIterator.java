/**
 * 
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
     * 
     * @return
     * @throws ServerOperationFailed
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
            // Just fetched the last element. Refill...
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
     * TODO: Update javadoc!
     * 
     * This method fetches a new chunk of <code>RecordDescription</code>
     * instances from the DOMS and replaces the container currently held by
     * <code>cachedRecordDescriptions</code> with a new one, containing the
     * <code>RecordDescription</code> instances.
     * 
     * @return <code>true</code> if the <code>cachedRecordDescriptions</code>
     *         attribute was updated with new <code>RecordDescription</code>
     *         instances and otherwise <code>false</code>
     * @throws ServerOperationFailed
     */
    private void fillCache() throws ServerOperationFailed {

        for (String summaBaseID : baseStates.keySet()) {

            fetchBaseRecordDescriptions(summaBaseID);
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

    // TODO: Scissored code from DOMSReadableStorage....

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
    /*
        private List<Record> getSingleBaseRecordsModifiedAfter(long timeStamp,
                String summaBaseID, QueryOptions options)
                throws ServerOperationFailed, URISyntaxException {

            if (log.isTraceEnabled()) {
                log.trace("getSingleBaseRecordsModifiedAfter(long, String, "
                        + "QueryOptions): " + "called with timestamp: " + timeStamp
                        + " summaBaseID: " + summaBaseID + " QueryOptions: "
                        + options);
            }

            final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                    .get(summaBaseID);

            final URI collectionPID = baseConfiguration.getCollectionPID();
            final String viewID = baseConfiguration.getViewID();
            final URI contentModelEntryObjectPID = baseConfiguration
                    .getContentModelEntryObjectPID();

            final List<RecordDescription> recordDescriptions = domsClient
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
            if (log.isTraceEnabled()) {
                Logs.log(log, Logs.Level.TRACE,
                        "getSingleBaseRecordsModifiedAfter(long, String, "
                                + "QueryOptions): returning with modifiedRecords: "
                                + modifiedRecords);
            }
            return modifiedRecords;
        }
    */
}