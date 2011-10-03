package dk.statsbiblioteket.doms.integration.summa;

import dk.statsbiblioteket.doms.integration.summa.exceptions.RegistryFullException;
import dk.statsbiblioteket.doms.integration.summa.exceptions.UnknownKeyException;
import dk.statsbiblioteket.doms.integration.summa.registry.SelfCleaningObjectRegistry;
import dk.statsbiblioteket.summa.common.Record;
import dk.statsbiblioteket.summa.common.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;


import java.io.File;
import java.net.URISyntaxException;
import java.util.Iterator;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by IntelliJ IDEA.
 * User: eab
 * Date: 6/7/11
 * Time: 11:09 AM
 * To change this template use File | Settings | File Templates.
 */
public class SelfCleaningObjectRegistryTest {
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
    /**
     * A registry containing all record iterators instantiated by methods
     * returning a key associated with an iterator over their result sets.
     */
    private SelfCleaningObjectRegistry<Iterator<Record>> recordIterators;
    private static final long THREE_MINUTES = 180000;


    public SelfCleaningObjectRegistryTest() throws URISyntaxException {
        testConfiguration = Configuration.load(new File(Thread.currentThread().getContextClassLoader().getResource("radioTVTestConfiguration.xml").toURI()).getAbsolutePath());
    }

    @Before
    public void setUp(){
        final Configuration configuration = testConfiguration;
        storage = new DOMSReadableStorage(configuration, new OfflineDOMSWSClient());
        recordIterators = new SelfCleaningObjectRegistry<Iterator<Record>>(THREE_MINUTES);
    }

    @Test
    public void testRegister(){
        byte[] bytes = new byte[3];
        long key = -1l;
        try {
            key = recordIterators.register(new RecordIterator());
        } catch (RegistryFullException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        assertTrue("Returned no key or invalid key, key value: " + key,
                key >= 0);
    }

    @Test
    public void testRemove() throws UnknownKeyException {
        byte[] bytes = new byte[3];
        long key = -1l;
        try {
            key = recordIterators.register(new RecordIterator());
        } catch (RegistryFullException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
            recordIterators.remove(key);
    }

    @Test(expected = UnknownKeyException.class)
    public void testRemoveFails() throws UnknownKeyException {
        recordIterators.remove(-1l);
        fail("expected exception not thrown: 'UnknownKeyException'");
    }

    private final class RecordIterator implements Iterator<Record>{

        @Override
        public boolean hasNext() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Record next() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void remove() {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

}
