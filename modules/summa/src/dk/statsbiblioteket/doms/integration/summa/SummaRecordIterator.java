/**
 * 
 */
package dk.statsbiblioteket.doms.integration.summa;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dk.statsbiblioteket.doms.centralWebservice.RecordDescription;
import dk.statsbiblioteket.doms.client.DOMSWSClient;
import dk.statsbiblioteket.doms.client.ServerOperationFailed;
import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.storage.api.QueryOptions;
import dk.statsbiblioteket.util.Logs;

/**
 * FIXME! It is probably better to have a RecordDescription iterator and let the
 * DOMSReadableStorage build the Summa records. Thus, keeping all Summa-stuff in
 * the interface class.
 * 
 * @author Thomas Skou Hansen &lt;tsh@statsbiblioteket.dk&gt;
 */
class SummaRecordIterator implements Iterator<Record> {

    private static final Log log = LogFactory.getLog(SummaRecordIterator.class);

    /**
     * The client, connected to the DOMS server to retrieve objects from.
     */
    private final DOMSWSClient domsClient;

    private final List<BaseDOMSConfiguration> baseConfigurations;

    private final long startTimeStamp;
    private long previousModificationTimeStamp;

    private final QueryOptions queryOptions;

    SummaRecordIterator(DOMSWSClient domsClient,
	    List<BaseDOMSConfiguration> baseConfigurations, long timeStamp,
	    QueryOptions options) {

	this.domsClient = domsClient;
	this.baseConfigurations = baseConfigurations;
	startTimeStamp = timeStamp;
	queryOptions = options;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
	// Do a "next" without throwing exceptions and cache the result.

	// TODO Auto-generated method stub
	return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public Record next() {
	// Return the cached result from hasNext, if available. Remember
	// clearing the cache before returning.

	// TODO Auto-generated method stub
	return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
	// TODO Auto-generated method stub

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
     * private List<Record> getSingleBaseRecordsModifiedAfter(long timeStamp,
     * BaseDOMSConfiguration baseConfiguration, QueryOptions options) throws
     * ServerOperationFailed, URISyntaxException {
     * 
     * if (log.isTraceEnabled()) {
     * log.trace("getSingleBaseRecordsModifiedAfter(long, String, " +
     * "QueryOptions): " + "called with timestamp: " + timeStamp +
     * " baseConfiguration: " + baseConfiguration + " QueryOptions: " +
     * options); }
     * 
     * final URI collectionPID = baseConfiguration.getCollectionPID(); final
     * String viewID = baseConfiguration.getViewID(); final URI
     * contentModelEntryObjectPID = baseConfiguration
     * .getContentModelEntryObjectPID();
     * 
     * final List<RecordDescription> recordDescriptions = domsClient
     * .getModifiedEntryObjects(collectionPID, viewID,
     * contentModelEntryObjectPID, timeStamp, "Published"); // FIXME! Hard-coded
     * "Published" state. What about an enum?
     * 
     * // FIXME! Clarify how QueryOptions should be handled and // implement
     * filter-magic here...
     * 
     * // Trivial first-shot record and iterator construction. final
     * List<Record> modifiedRecords = new LinkedList<Record>(); for
     * (RecordDescription recordDescription : recordDescriptions) {
     * 
     * // Get the PID of the modified content model entry object. final URI
     * modifiedEntryCMObjectPID = new URI(recordDescription .getPid()); final
     * byte data[] = domsClient.getViewBundle( modifiedEntryCMObjectPID,
     * viewID).getBytes();
     * 
     * // Prepend the base name to the PID in order to make it possible // for
     * the getRecord() methods to figure out what view to use // when they are
     * invoked. It's ugly, but hey! That's life.... final String summaRecordID =
     * summaBaseID + RECORD_ID_DELIMITER + modifiedEntryCMObjectPID.toString();
     * final Record newRecord = new Record(summaRecordID, summaBaseID, data);
     * modifiedRecords.add(newRecord); } if (log.isTraceEnabled()) {
     * Logs.log(log, Logs.Level.TRACE,
     * "getSingleBaseRecordsModifiedAfter(long, String, " +
     * "QueryOptions): returning with modifiedRecords: " + modifiedRecords); }
     * return modifiedRecords; }
     */
}
