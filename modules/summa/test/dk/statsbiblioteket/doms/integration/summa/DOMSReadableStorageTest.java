/**
 * 
 */
package dk.statsbiblioteket.doms.integration.summa;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import dk.statsbiblioteket.summa.common.configuration.SubConfigurationsNotSupportedException;

/**
 * @author &lt;tsh@statsbiblioteket.dk&gt;
 */
public class DOMSReadableStorageTest {

    private static final String TEST_CONFIGURATION_XML_FILE_PATH = "./config/radioTVTestConfiguration.xml";
    private DOMSReadableStorage storage;
    private final Configuration testConfiguration;

    public DOMSReadableStorageTest() {
        testConfiguration = getConfiguration();
    }

    /**
     * Ensure a pristine <code>DOMSReadableStorage</code> instance for each
     * test.
     * 
     * @throws java.lang.Exception
     *             if the <code>DOMSReadableStorage</code> could not be created.
     */
    @Before
    public void setUp() throws Exception {
        storage = new DOMSReadableStorage(testConfiguration);
    }

    /**
     * TODO: Remove this method if there is no need for tearing anything down.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for
     * {@link dk.statsbiblioteket.doms.integration.summa.DOMSReadableStorage#getModificationTime(java.lang.String)}
     * .
     * 
     * This test will succeed if the time-stamp returned by
     * <code>getModificationTime(base)</code> returns a value larger than zero.
     */
    @Test
    public void testGetModificationTime() {
        try {

            final String baseID = getTestBaseID();

            assertFalse(
                    "The latest modification time of the collection (base='"
                            + baseID + "') was implausible old.", storage
                            .getModificationTime(baseID) <= 0);

        } catch (Exception exception) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final PrintStream failureMessage = new PrintStream(bos);
            failureMessage.print("testNextLong(): Caught exception: ");
            exception.printStackTrace(failureMessage);
            failureMessage.flush();
            fail(bos.toString());
        }
    }

    /**
     * Test method for
     * {@link dk.statsbiblioteket.doms.integration.summa.DOMSReadableStorage#getModificationTime(java.lang.String)}
     * . Verify that the method handles a <code>null</code> base name properly.
     * This test will succeed if the time-stamp returned by
     * <code>getModificationTime(null)</code> returns a value larger than zero.
     */
    @Test
    public void testGetModificationTimeNullBase() {
        try {

            assertFalse("The latest modification time of the collection (base="
                    + "<null>) was implausible old.", storage
                    .getModificationTime(null) <= 0);

        } catch (Exception exception) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final PrintStream failureMessage = new PrintStream(bos);
            failureMessage.print("testNextLong(): Caught exception: ");
            exception.printStackTrace(failureMessage);
            failureMessage.flush();
            fail(bos.toString());
        }
    }

    /**
     * Test method for
     * {@link dk.statsbiblioteket.doms.integration.summa.DOMSReadableStorage#getRecordsModifiedAfter(long, java.lang.String, dk.statsbiblioteket.summa.storage.api.QueryOptions)}
     * .
     * 
     * This test will succeed if <code>getRecordModifiesAfter()</code> returns
     * an iterator key when it is invoked with a zero time-stamp (the beginning
     * of time) and the test base ID returned by the
     * <code>getTestBaseID()</code> helper method in this test class.
     */
    @Test
    public void testGetRecordsModifiedAfter() {
        try {
            final String baseID = getTestBaseID();
            final long SINCE_ANCIENT_TIMES = 0;
            final long iteratorKey = storage.getRecordsModifiedAfter(
                    SINCE_ANCIENT_TIMES, baseID, null);

            // FIXME! Test various QueryOptions.

            // Expect at least one element in the configured collection.
            assertNotNull(storage.next(iteratorKey));

        } catch (Exception exception) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final PrintStream failureMessage = new PrintStream(bos);
            failureMessage.print("testNextLong(): Caught exception: ");
            exception.printStackTrace(failureMessage);
            failureMessage.flush();
            fail(bos.toString());
        }
    }

    /**
     * Test method for
     * {@link dk.statsbiblioteket.doms.integration.summa.DOMSReadableStorage#getRecordsModifiedAfter(long, java.lang.String, dk.statsbiblioteket.summa.storage.api.QueryOptions)}
     * .
     * 
     * This test will succeed if <code>getRecordModifiesAfter()</code> returns
     * an iterator key when it is invoked with a zero time-stamp (the beginning
     * of time) and a <code>null</code> base ID.
     * <p/>
     * The method under test is supposed to return an iterator over all modified
     * elements in all known bases when the base ID is <code>null</code>.
     */
    @Test
    public void testGetRecordsModifiedAfterNullBase() {
        try {
            final long SINCE_ANCIENT_TIMES = 0;
            final long iteratorKey = storage.getRecordsModifiedAfter(
                    SINCE_ANCIENT_TIMES, null, null);

            // FIXME! Test various QueryOptions.

            // Expect at least one element in the configured collection.
            assertNotNull(storage.next(iteratorKey));

        } catch (Exception exception) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final PrintStream failureMessage = new PrintStream(bos);
            failureMessage.print("testNextLong(): Caught exception: ");
            exception.printStackTrace(failureMessage);
            failureMessage.flush();
            fail(bos.toString());
        }
    }

    /**
     * Test method for
     * {@link dk.statsbiblioteket.doms.integration.summa.DOMSReadableStorage#next(long, int)}
     * .
     * 
     * This test will succeed if <code>next(long, int)</code> on the
     * DOMSReadableStorage returns any records, given an iterator key for
     * iteration over all records in the base returned by the
     * <code>getTestBaseID()</code> helper method in this test class. Thus, the
     * DOMS server associated with this base ID in the test configuration must
     * contain some objects in order to make this test run successfully.
     */
    @Test
    public void testNextLongInt() {

        try {
            final String baseID = getTestBaseID();
            final long SINCE_ANCIENT_TIMES = 0;
            final long iteratorKey = storage.getRecordsModifiedAfter(
                    SINCE_ANCIENT_TIMES, baseID, null);

            // Expect at least one element in the configured collection.
            assertFalse(storage.next(iteratorKey, Integer.MAX_VALUE).isEmpty());
        } catch (Exception exception) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final PrintStream failureMessage = new PrintStream(bos);
            failureMessage.print("testNextLongInt(): Caught exception: ");
            exception.printStackTrace(failureMessage);
            failureMessage.flush();
            fail(bos.toString());
        }
    }

    /**
     * Test method for
     * {@link dk.statsbiblioteket.doms.integration.summa.DOMSReadableStorage#next(long)}
     * .
     * 
     * This test will succeed if <code>next(long)</code> on the
     * DOMSReadableStorage returns any records, given an iterator key for
     * iteration over all records in the base returned by the
     * <code>getTestBaseID()</code> helper method in this test class. Thus, the
     * DOMS server associated with this base ID in the test configuration must
     * contain some objects in order to make this test run successfully.
     */
    @Test
    public void testNextLong() {
        try {
            // Get an iterator over all modified records.
            final String baseID = getTestBaseID();
            final long SINCE_ANCIENT_TIMES = 0;
            final long iteratorKey = storage.getRecordsModifiedAfter(
                    SINCE_ANCIENT_TIMES, baseID, null);

            // Just trash the returned record. There is no way to validate it
            // anyway.
            assertNotNull(storage.next(iteratorKey));
        } catch (Exception exception) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final PrintStream failureMessage = new PrintStream(bos);
            failureMessage.print("testNextLong(): Caught exception: ");
            exception.printStackTrace(failureMessage);
            failureMessage.flush();
            fail(bos.toString());
        }
    }

    /**
     * Test method for
     * {@link dk.statsbiblioteket.doms.integration.summa.DOMSReadableStorage#getRecord(java.lang.String, dk.statsbiblioteket.summa.storage.api.QueryOptions)}
     * . This test fetches the first record returned by calling
     * <code>getRecordsModifiedAfter()</code> and <code>next()</code>, retrieves
     * its ID and tests the <code>getRecord(id, option)</code> by calling it
     * with the retrieved ID. The returned record is compared with the original
     * instance returned by <code>next()</code>. Hence, the DOMS server
     * specified in the configuration must contain at least one object in order
     * to execute this test successfully.
     */
    @Test
    public void testGetRecord() {

        try {
            final String baseID = getTestBaseID();
            final long SINCE_ANCIENT_TIMES = 0;
            final long iteratorKey = storage.getRecordsModifiedAfter(
                    SINCE_ANCIENT_TIMES, baseID, null);

            // FIXME! Test various QueryOptions.

            // Expect at least one element in the configured collection.
            final Record anyRecord = storage.next(iteratorKey);

            final Record sameRecord = storage
                    .getRecord(anyRecord.getId(), null);
            assertEquals(
                    "Failed performing an explicit retrieval of the record returned by the iterator.",
                    anyRecord, sameRecord);
        } catch (Exception exception) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final PrintStream failureMessage = new PrintStream(bos);
            failureMessage.print("testNextLong(): Caught exception: ");
            exception.printStackTrace(failureMessage);
            failureMessage.flush();
            fail(bos.toString());
        }
    }

    /**
     * Test method for
     * {@link dk.statsbiblioteket.doms.integration.summa.DOMSReadableStorage#getRecords(java.util.List, dk.statsbiblioteket.summa.storage.api.QueryOptions)}
     * . This test fetches the first three records returned by calling
     * <code>getRecordsModifiedAfter()</code> and <code>next()</code>, retrieves
     * their ID and tests the
     * <code>getRecords(List&lt;String&gt; ids, QueryOption option)</code> by
     * calling it with the retrieved IDs. The returned records are compared with
     * the original instances returned by <code>next()</code>. Hence, the DOMS
     * server specified in the configuration must contain at least three objects
     * in order to execute this test successfully.
     */
    @Test
    public void testGetRecords() {
        try {

            // Get an iterator over all modified records.
            final String baseID = getTestBaseID();
            final long SINCE_ANCIENT_TIMES = 0;
            final long iteratorKey = storage.getRecordsModifiedAfter(
                    SINCE_ANCIENT_TIMES, baseID, null);

            // FIXME! Test various QueryOptions.

            // Expect that there are least three elements in the configured
            // collection and collect them and their summa ID.
            final List<Record> iteratorReords = new LinkedList<Record>();
            final List<String> summaIDs = new LinkedList<String>();
            for (int i = 0; i < 3; i++) {
                final Record aRecord = storage.next(iteratorKey);
                iteratorReords.add(aRecord);
                summaIDs.add(aRecord.getId());
            }

            // Get the same records once again, using the getRecords() method.
            List<Record> recordsFromGet = storage.getRecords(summaIDs, null);
            assertEquals(
                    "The records returned by getRecords() are not equal to "
                            + "the ones returned by the iterator.",
                    iteratorReords, recordsFromGet);

        } catch (Exception exception) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final PrintStream failureMessage = new PrintStream(bos);
            failureMessage.print("testNextLong(): Caught exception: ");
            exception.printStackTrace(failureMessage);
            failureMessage.flush();
            fail(bos.toString());
        }
    }

    private Configuration getConfiguration() {
        return Configuration.load(TEST_CONFIGURATION_XML_FILE_PATH);
    }

    private String getTestBaseID()
            throws SubConfigurationsNotSupportedException {
        final List<Configuration> baseConfigurations = testConfiguration
                .getSubConfigurations(ConfigurationKeys.ACCESSIBLE_COLLECTION_BASES);

        assertFalse(
                "There are no collection base definitions in the configuration file.",
                baseConfigurations.isEmpty());

        // Just use the first collection base information element.
        return baseConfigurations.get(0).getString(
                ConfigurationKeys.COLLECTION_BASE_ID);
    }

}
