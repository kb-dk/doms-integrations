package dk.statsbiblioteket.doms.integration.summa;


import dk.statsbiblioteket.doms.integration.summa.parsing.ConfigurationKeys;
import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import dk.statsbiblioteket.summa.common.configuration.SubConfigurationsNotSupportedException;
import dk.statsbiblioteket.summa.storage.api.QueryOptions;
import org.apache.commons.lang.NotImplementedException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by IntelliJ IDEA.
 * User: eab
 * Date: 4/29/11
 * Time: 12:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class OfflineDOMSReadableStorageTest {


    /**
     * The current <code>DOMSReadableStorage</code> instance under test.
     */
    private DOMSReadableStorage storage;

    /**
     * The test configuration loaded by the constructor, from which the
     * individual test methods may fetch information which is necessary for
     * their execution.
     */
    private Configuration testConfiguration;

    @Before
    public void setUp() throws URISyntaxException {
        testConfiguration = Configuration.load(new File(Thread.currentThread().getContextClassLoader().getResource("radioTVTestConfiguration.xml").toURI()).getAbsolutePath());
        storage = new DOMSReadableStorage(testConfiguration, new OfflineDOMSWSClient());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetRecordIllegalArgumentException()  {
        try {
            Record r = storage.getRecord("ID:1", new QueryOptions());
            fail("getStorage() should fail with an exception");
        } catch (IOException e) {
        }
    }

    @Test
    public void testGetRecord() throws IOException {
        Record r = storage.getRecord("doms_radioTVCollection:1", new QueryOptions());
        assertEquals("Returned object is not a Record", r.getClass(), Record.class);
    }

    @Test
    public void testGetRecordsReturnsList() throws IOException{
        List<String> ids = new ArrayList<String>();
        ids.add("doms_radioTVCollection:1");
        List<Record> rs = storage.getRecords(ids, new QueryOptions());
        assertFalse("Returned object is not a list", rs.isEmpty());
    }

    @Test
    public void testGetRecordsContentIsRecord() throws IOException{
        List<String> ids = new ArrayList<String>();
        ids.add("doms_radioTVCollection:1");
        List<Record> rs = storage.getRecords(ids, new QueryOptions());
        assertEquals("List does not contain Recors classes",
                rs.get(0).getClass(), Record.class);
    }



    @Test
    public void testGetModificationTime(){
        try {
            final String baseID = getFirstBaseID(testConfiguration);

            assertFalse(
                    "The latest modification time of the collection (base='"
                            + baseID + "') was implausible old.", storage
                            .getModificationTime(baseID) < 0);

        } catch (Exception ex){
            System.out.print(ex);
        }
    }

    @Test
    public void testGetModificationTimeIDNull(){
        String baseID = null;
        try {
        assertTrue("The latest modification time of the collection (base='"
                            + baseID + "') was implausible old.", storage
                            .getModificationTime(baseID) == 0);
        } catch (Exception ex) {
            System.out.print(ex);
        }
    }

    @Test
    public void testGetRecordsModifiedAfterNoBaseIDNoOptions(){
        long timeStamp = 0l;
        String summaBaseID = null;
        QueryOptions options = null;
        try {
            assertTrue(storage.getRecordsModifiedAfter(timeStamp, summaBaseID,
                    options) >= 0);
        } catch (Exception ex) {
            System.out.print(ex);
        }
    }

    @Test
    public void  testGetRecordModifiedAfterNoOptions(){
        long timeStamp = 0l;
        String summaBaseID = "doms_RadioTVCollection";
        QueryOptions options = null;
        try {
            assertTrue(storage.getRecordsModifiedAfter(timeStamp, summaBaseID,
                    options) >= 0);
        } catch (Exception ex) {
            System.out.print(ex);
        }
    }


    @Test
    public void testNextIsNull() throws IOException {
        long timeStamp = 0l;
        String summaBaseID = "doms_radioTVCollection";
        QueryOptions options = null;
        long iterKey = iterKey = storage.getRecordsModifiedAfter(timeStamp, summaBaseID,
               options);
        assertNotNull(storage.next(iterKey));
    }

    //@Test
    public void testNextGivesEmptyList() throws IOException {
        long timeStamp = 0l;
        String summaBaseID = "doms_radioTVCollection";
        QueryOptions options = null;
        long iterKey = 0l;
        assertNull(storage.next(iterKey, 1).get(0));
    }

    @Test(expected = NotImplementedException.class)
    public void testBatchJob() throws IOException {
        storage.batchJob("test", "test", 0l, 0l, new QueryOptions());
        fail("Someone implemented the method, please update tests");
    }

    @Test(expected = NotImplementedException.class)
    public void testClearBase() throws IOException {
        storage.clearBase("test");
        fail("Someone implemented the method, please update tests");
    }

    @Test(expected = NotImplementedException.class)
    public void testClose() throws IOException {
        storage.close();
        fail("Someone implemented the method, please update tests");
    }

    @Test(expected = NotImplementedException.class)
    public void testFlushAll() throws IOException {
        storage.flushAll(new ArrayList<Record>());
        fail("Someone implemented the method, please update tests");
    }

    @Test(expected = NotImplementedException.class)
    public void testFlushAllWithOptions() throws IOException {
        storage.flushAll(new ArrayList<Record>(), new QueryOptions(false, false, 0, 0));
        fail("Someone implemented the method, please update tests");
    }                                      //@Test


    @Test(expected = NotImplementedException.class)
    public void testFlushWithOptions() throws IOException {
        storage.flush(new Record("test", "test", new byte[0]), new QueryOptions(false, false, 0, 0));
        fail("Someone implemented the method, please update tests");
    }

    @Test(expected = NotImplementedException.class)
    public void testFlush() throws IOException {
        storage.flush(new Record("test", "test", new byte[0]));
        fail("Someone implemented the method, please update tests");
    }

    /**
     * Fetch the Summa base ID from the first base configuration found in
     * <code>configuration</code>.
     *
     * @return The Summa base ID of the first base configuration found in
     *         <code>configuration</code>.
     * @throws dk.statsbiblioteket.summa.common.configuration.SubConfigurationsNotSupportedException
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

