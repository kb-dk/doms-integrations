package dk.statsbiblioteket.doms.integration.summa;


import dk.statsbiblioteket.doms.client.DomsWSClientImpl;
import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import dk.statsbiblioteket.summa.common.configuration.SubConfigurationsNotSupportedException;
import dk.statsbiblioteket.summa.storage.api.QueryOptions;
import org.apache.commons.lang.NotImplementedException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
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

    public OfflineDOMSReadableStorageTest(){
        System.out.println(new File(TEST_CONFIGURATION_XML_FILE_PATH).getAbsolutePath());
        testConfiguration = Configuration
                .load(TEST_CONFIGURATION_XML_FILE_PATH);
    }
    /**
     * Path to the configuration file used by the tests in this test class.
     */
    private static final String TEST_CONFIGURATION_XML_FILE_PATH =
            "src/test/resources/radioTVTestConfiguration.xml";
    /**
     * The current <code>DOMSReadableStorage</code> instance under test.
     */
    private OfflineDOMSReadableStorage storage;

    /**
     * The test configuration loaded by the constructor, from which the
     * individual test methods may fetch information which is necessary for
     * their execution.
     */
    private final Configuration testConfiguration;

    @Before
    public void setUp(){
        storage = new OfflineDOMSReadableStorage(testConfiguration);

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
    @Ignore // Ignored due to unavoidable network calls
    public void testGetModificationTime(){
        try {
            final String baseID = getFirstBaseID(testConfiguration);

        //storage.domsClient.modificationTime = 1;
            assertFalse(
                    "The latest modification time of the collection (base='"
                            + baseID + "') was implausible old.", storage
                            .getModificationTime(baseID) <= 0);

        } catch (Exception ex){
            System.out.print(ex);
        }

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
    }

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

class OfflineDOMSReadableStorage extends DOMSReadableStorage{

    /**
     * Create a <code>DOMSReadableStorage</code> instance based on the
     * configuration provided by <code>configuration</code>.
     *
     * @param configuration Configuration containing information for mapping Summa base
     *                      names to DOMS collections and views.
     * @throws dk.statsbiblioteket.summa.common.configuration.Configurable.ConfigurationException
     *          if the configuration contains any errors regarding option
     *          values or document structure.
     */
    public OfflineDOMSReadableStorage(Configuration configuration) throws ConfigurationException {
        super(configuration);
    }


    @Override
    protected DomsWSClientImpl domsLogin(Configuration config){
        return new OfflineDOMSWSClient();
    }





}

class OfflineDOMSWSClient extends DomsWSClientImpl{

    public long modificationTime;
    public OfflineDOMSWSClient(){
        modificationTime = 0;
    }

    @Override
    public long getModificationTime(String collectionPID, String viewID,
                                    String state){
        return modificationTime;
    }

//    @Override
    public String getViewBundle(String entryObjectPID, String viewID){
        return viewID+":"+entryObjectPID;
    }
}