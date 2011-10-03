/**
 * 
 */
package dk.statsbiblioteket.doms.integration.summa;

import dk.statsbiblioteket.doms.client.DomsWSClientImpl;
import dk.statsbiblioteket.doms.integration.summa.parsing.ConfigurationKeys;
import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import dk.statsbiblioteket.summa.common.configuration.SubConfigurationsNotSupportedException;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 *
 * This test requires a running doms system to connect to, at the location specified in the config file
 * @author Thomas Skou Hansen &lt;tsh@statsbiblioteket.dk&gt;
 */
public class DOMSReadableStorageTest {


    /**
     * The current <code>DOMSReadableStorage</code> instance under test.
     */
    private DOMSReadableStorage storage;

    /**
     * The test configuration loaded by the constructor, from which the
     * individual test methods may fetch information which is necessary for
     * their execution.
     */
    private final Configuration testConfiguration;

    public DOMSReadableStorageTest() throws URISyntaxException {
        testConfiguration = Configuration.load(new File(Thread.currentThread().getContextClassLoader().getResource("radioTVTestConfiguration.xml").toURI()).getAbsolutePath());


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
        storage = new DOMSReadableStorage(testConfiguration, new DomsWSClientImpl());
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

            final String baseID = getFirstBaseID(testConfiguration);

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
            final String baseID = getFirstBaseID(testConfiguration);
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
            final String baseID = getFirstBaseID(testConfiguration);
            final long SINCE_ANCIENT_TIMES = 0;
            final long iteratorKey = storage.getRecordsModifiedAfter(
                    SINCE_ANCIENT_TIMES, baseID, null);

            // Expect at least one element in the configured collection and get
            // all elements.
            assertFalse(storage.next(iteratorKey, Integer.MAX_VALUE).isEmpty());

            try {
                // Verify that the iterator is depleted.
                storage.next(iteratorKey, Integer.MAX_VALUE);

                // Oops. the iterator was supposed to be depleted.
                fail("The iterator (key = " + iteratorKey
                        + ") was not empty nor deleted from the storage.");

            } catch (IllegalArgumentException illegalArgumentException) {
                assertTrue(true);
            } catch (NoSuchElementException noSuchElementException) {
                // No more elements... that was just what we wanted.
            }

            try {
                // Expect that the iterator we just emptied has been removed
                // from the storage.
                storage.next(iteratorKey, Integer.MAX_VALUE);
            } catch (IllegalArgumentException illegalArgumentException) {
                assertTrue(true);
            } catch (NoSuchElementException noSuchElementException) {
                // Oops. the iterator was supposed to be removed from the
                // storage.
                fail("The iterator (key = " + iteratorKey
                        + ") was not deleted from the storage.");
            }
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
            final String baseID = getFirstBaseID(testConfiguration);
            final long SINCE_ANCIENT_TIMES = 0;
            final long iteratorKey = storage.getRecordsModifiedAfter(
                    SINCE_ANCIENT_TIMES, baseID, null);

            try {

                // Make sure that the iterator is depleted...
                for (int i = 0; i <= Integer.MAX_VALUE; i++) {
                    // Just trash the returned record. There is no way to
                    // validate it
                    // anyway.
                    assertNotNull(storage.next(iteratorKey));
                }

                fail("The iterator (key = " + iteratorKey
                        + ") did not terminate properly.");
            } catch (NoSuchElementException noSuchElementException) {
                // Fine. That is just what we want...
            }

            try {
                // Expect that the iterator we just emptied has been removed
                // from the storage.
                storage.next(iteratorKey);

                // Oops. the iterator was supposed to be depleted.
                fail("The iterator (key = " + iteratorKey
                        + ") was not empty nor deleted from the storage.");

            } catch (IllegalArgumentException illegalArgumentException) {
                // Fine, the iterator was unknown to the storage.
                assertTrue(true);
            } catch (NoSuchElementException noSuchElementException) {
                // Oops. the iterator was supposed to be removed from the
                // storage.
                fail("The iterator (key = " + iteratorKey
                        + ") was not deleted from the storage.");
            }
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
            final String baseID = getFirstBaseID(testConfiguration);
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
            final String baseID = getFirstBaseID(testConfiguration);
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

    /**
     * Fetch the Summa base ID from the first base configuration found in
     * <code>configuration</code>.
     * 
     * @return The Summa base ID of the first base configuration found in
     *         <code>configuration</code>.
     * @throws SubConfigurationsNotSupportedException
     *             if the configuration does not contain a
     *             <code>accessibleCollectionBases</code> section;
     */
    private String getFirstBaseID(Configuration configuration)
            throws SubConfigurationsNotSupportedException {

        final List<Configuration> baseConfigurations = configuration
                .getSubConfigurations(ConfigurationKeys.ACCESSIBLE_COLLECTION_BASES);

        assertFalse(
                "There are no collection base definitions in the configuration file.",
                baseConfigurations.isEmpty());

        // Just use the first collection base information element.
        return baseConfigurations.get(0).getString(
                ConfigurationKeys.COLLECTION_BASE_ID);
    }
}
