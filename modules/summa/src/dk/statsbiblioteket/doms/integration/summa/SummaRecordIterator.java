/**
 * 
 */
package dk.statsbiblioteket.doms.integration.summa;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

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

    /**
     * The client, connected to the DOMS server to retrieve objects from.
     */
    private final DOMSWSClient domsClient;

    private final Map<String, BaseDOMSConfiguration> baseConfigurations;
    private final Iterator<String> summaBaseIDIterator;
    private final long startTimeStamp;
    private final QueryOptions queryOptions;

    private String currentSummaBaseID;
    private long currentRecordIndex;

    private Record cachedRecord;

    SummaRecordIterator(DOMSWSClient domsClient,
            Map<String, BaseDOMSConfiguration> baseConfigurations,
            Set<String> summaBaseIDs, long timeStamp, QueryOptions options) {

        this.domsClient = domsClient;
        this.baseConfigurations = baseConfigurations;
        summaBaseIDIterator = summaBaseIDs.iterator();
        startTimeStamp = timeStamp;
        queryOptions = options;
        cachedRecord = null;
        currentSummaBaseID = summaBaseIDIterator.next();
        currentRecordIndex = 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() {
        if (cachedRecord != null) {
            return true;
        } else {
            // Do a "next" without throwing exceptions and cache the result.
            try {
                cachedRecord = next();
                return true;
            } catch (NoSuchElementException noSuchElementException) {
                // Ditch the exception.
                cachedRecord = null;
                return false;
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    public Record next() {

        // TODO: It may be better fetching a small chunk of records, rather than
        // making a web service call per record!
        if (cachedRecord != null) {
            final Record resultRecord = cachedRecord;
            cachedRecord = null;
            return resultRecord;
        } else {

            try {
                // Note! getNextRecordDescription() may modify attributes such
                // as currentSummaBaseID!
                final RecordDescription recordDescription = getNextRecordDescription();

                final URI modifiedEntryObjectPID = new URI(recordDescription
                        .getPid());

                final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                        .get(currentSummaBaseID);

                final String viewID = baseConfiguration.getViewID();

                final byte data[] = domsClient.getViewBundle(
                        modifiedEntryObjectPID, viewID).getBytes();

                // Prepend the base name to the PID in order to make it possible
                // for the DOMSReadableStorage.getRecord() methods to figure out
                // what view to use when they are invoked. It's ugly, but hey!
                // That's life....
                final String summaRecordID = currentSummaBaseID
                        + DOMSReadableStorage.RECORD_ID_DELIMITER
                        + modifiedEntryObjectPID.toString();
                final Record newRecord = new Record(summaRecordID,
                        currentSummaBaseID, data);

                return newRecord;
            } catch (URISyntaxException uriSyntaxException) {
                final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                        .get(currentSummaBaseID);

                final String errorMessage = "Failed retrieving record "
                        + "(startTime = " + startTimeStamp + " index = "
                        + (currentRecordIndex - 1) + " viewID = "
                        + baseConfiguration.getViewID()
                        + ") from collection (PID = "
                        + baseConfiguration.getCollectionPID() + ").";
                log.warn("next(): " + errorMessage, uriSyntaxException);
                throw new NoSuchElementException(errorMessage);
            } catch (ServerOperationFailed serverOperationFailed) {
                final BaseDOMSConfiguration baseConfiguration = baseConfigurations
                        .get(currentSummaBaseID);

                final String errorMessage = "Failed retrieving record "
                        + "(startTime = " + startTimeStamp + " index = "
                        + (currentRecordIndex - 1) + " viewID = "
                        + baseConfiguration.getViewID()
                        + ") from collection (PID = "
                        + baseConfiguration.getCollectionPID() + ").";
                log.warn("next(): " + errorMessage, serverOperationFailed);
                throw new NoSuchElementException(errorMessage);
            }
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
     * @return
     * @throws ServerOperationFailed
     */
    private RecordDescription getNextRecordDescription()
            throws ServerOperationFailed {

        BaseDOMSConfiguration baseConfiguration = baseConfigurations
                .get(currentSummaBaseID);

        URI collectionPID = baseConfiguration.getCollectionPID();
        String viewID = baseConfiguration.getViewID();
        final String objectState = "Published";// FIXME! Hard-coded object
        // state!

        try {
            List<RecordDescription> recordDescriptions = domsClient
                    .getModifiedEntryObjects(collectionPID, viewID,
                            startTimeStamp, objectState, currentRecordIndex++,
                            1);

            // If there are no more record descriptions available, the iterate
            // through the remaining bases/collections before throwing in the
            // towel.
            while (recordDescriptions.size() == 0
                    && summaBaseIDIterator.hasNext()) {

                // Attempt to get a record description, using the next Summa
                // base ID.
                currentSummaBaseID = summaBaseIDIterator.next();

                baseConfiguration = baseConfigurations.get(currentSummaBaseID);
                collectionPID = baseConfiguration.getCollectionPID();
                viewID = baseConfiguration.getViewID();

                recordDescriptions = domsClient.getModifiedEntryObjects(
                        collectionPID, viewID, startTimeStamp, objectState,
                        currentRecordIndex++, 1);
            }

            if (recordDescriptions.isEmpty()) {
                // We\re out of Summa base IDs and record descriptions...
                throw new NoSuchElementException(
                        "This iterator is out of records.");
            }

            return recordDescriptions.get(0);

        } catch (ServerOperationFailed serverOperationFailed) {
            final String errorMessage = "Failed retrieving record "
                    + "(startTime = " + startTimeStamp + " index = "
                    + (currentRecordIndex - 1) + " viewID = " + viewID
                    + " objectState = " + objectState
                    + ") from collection (PID = " + collectionPID + ").";
            log.warn("getNextRecordDescription(): " + errorMessage,
                    serverOperationFailed);
            throw serverOperationFailed;
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