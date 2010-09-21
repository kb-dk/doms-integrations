/**
 * 
 */
package dk.statsbiblioteket.doms.integration.summa;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import dk.statsbiblioteket.summa.common.configuration.storage.FileStorage;

/**
 * @author tsh
 * 
 */
public class DOMSReadableStorageTest {

    private static final String TEST_CONFIGURATION_XML = "./config/radioTVTestConfiguration.xml";
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

    // FIXME! This test is a fake for generation of the test configuration and
    // thus it should be removed when the configuration is complete.
    @Test
    public void writeTestConfig() {
	try {
	    FileStorage configurationStorage = new FileStorage(new File(
		    TEST_CONFIGURATION_XML));
	    Configuration configuration = new Configuration(
		    configurationStorage);
	    configuration.set(ConfigurationKeys.COLLECTION_BASE_ID,
		    "doms:RadioTV_Collection");
	    // Don't worry, be happy....
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
     * {@link dk.statsbiblioteket.doms.integration.summa.DOMSReadableStorage#getModificationTime(java.lang.String)}
     * .
     */
    @Test
    public void testGetModificationTime() {
	try {
	    assertTrue(
		    "The latest modification time of the collection was implausible old.",
		    storage.getModificationTime(testConfiguration
		            .getString(ConfigurationKeys.COLLECTION_BASE_ID)) == 0);
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
	    // Just trash the returned key. There is no way to validate it
	    // anyway.
	    storage.getRecordsModifiedAfter(0, testConfiguration
		    .getString(ConfigurationKeys.COLLECTION_BASE_ID), null);
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
	    assertNull(records);
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
	return Configuration.load(TEST_CONFIGURATION_XML);
    }
}
