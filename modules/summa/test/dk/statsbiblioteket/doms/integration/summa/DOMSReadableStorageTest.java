/**
 * 
 */
package dk.statsbiblioteket.doms.integration.summa;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import dk.statsbiblioteket.summa.common.configuration.SubConfigurationsNotSupportedException;

/**
 * @author tsh
 * 
 */
public class DOMSReadableStorageTest {

    private static final String TEST_CONFIGURATION_XML_FILE_PATH = "./config/radioTVTestConfiguration.xml";
    private DOMSReadableStorage storage;
    private final Configuration testConfiguration;

    public DOMSReadableStorageTest() {
	testConfiguration = getConfiguration();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
	storage = new DOMSReadableStorage(testConfiguration);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for
     * {@link dk.statsbiblioteket.doms.integration.summa.DOMSReadableStorage#getModificationTime(java.lang.String)}
     * .
     */
    @Test
    public void testGetModificationTime() {
	try {

	    final String baseID = getTestBaseID();

	    assertFalse("The latest modification time of the collection (base='"
		    + baseID + "') was implausible old.", storage
		    .getModificationTime(baseID) == 0);
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
     * This test will either work and be successful or break violently.
     */
    @Test
    public void testGetRecordsModifiedAfter() {
	try {
	    final String baseID = getTestBaseID();
	    final long SINCE_ANCIENT_TIMES = 0;
	    final long iteratorKey = storage.getRecordsModifiedAfter(
		    SINCE_ANCIENT_TIMES, baseID, null);
	    // TODO: Test various QueryOptions.

	    // Verify that the iterator key works and returns something.
	    assertNull(storage.next(iteratorKey));

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
     * This test will either work and be successful or break violently.
     */
    @Test
    public void testNextLongInt() {
	try {
	    // Just trash the returned records. There is no way to validate them
	    // anyway.
	    storage.next(7, Integer.MAX_VALUE);
	    assertTrue(true);
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
     * .
     */
    @Test
    public void testGetRecord() {
	try {
	    Record record = storage.getRecord("doms:non-existent", null);
	    assertNull(record);
	    // TODO: Improve this test.
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
     * .
     */
    @Test
    public void testGetRecords() {
	try {
	    final String[] pidList = new String[] { "doms:1", "doms:2",
		    "doms:3" };
	    List<Record> records = storage.getRecords(Arrays.asList(pidList),
		    null);
	    assertNotNull(records);
	    assertTrue(records.isEmpty());
	    // TODO: Improve this test.
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
     * {@link dk.statsbiblioteket.doms.integration.summa.DOMSReadableStorage#next(long)}
     * .
     */
    @Test
    public void testNextLong() {
	try {
	    // Just trash the returned record. There is no way to validate them
	    // anyway.
	    storage.next(7);
	    assertTrue(true);
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
