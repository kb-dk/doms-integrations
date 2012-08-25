package dk.statsbiblioteket.doms.integration.summa;

import dk.statsbiblioteket.doms.central.RecordDescription;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: eab
 * Date: 6/8/11
 * Time: 9:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class BaseRecordDescriptionTest {

    RecordDescription originalRD = new RecordDescription();
    RecordDescription sameRD;
    RecordDescription identicalRD;
    RecordDescription differentRD;
    BaseRecordDescription originalBRD;
    BaseRecordDescription sameBRD;
    BaseRecordDescription identicalBRD;
    BaseRecordDescription differentBRD;
    String SUMMA_BASE_ID = "BaseID";


    public BaseRecordDescriptionTest(){


    }
    @Before
    public void setUp(){
        differentRD = new RecordDescription();
        differentRD.setDate(1l);
        originalRD.setDate(0l);
        sameRD = originalRD;
        originalBRD = new BaseRecordDescription(SUMMA_BASE_ID, originalRD);
        sameBRD = originalBRD;
        identicalBRD = new BaseRecordDescription(SUMMA_BASE_ID, originalRD);
        differentBRD = new BaseRecordDescription("DifferentID", differentRD);
    }

    @Test
    public void testCompareTo(){
        assertTrue("Failed when comparing two equal objects, value:" +
                originalBRD.compareTo(sameBRD),
                originalBRD.compareTo(sameBRD) == 0);
        assertTrue("Failed when comparing two equal objects, value:" +
                originalBRD.compareTo(identicalBRD),
                originalBRD.compareTo(identicalBRD) == 0);
        assertTrue("Failed when comparing two equal objects, value:" +
                originalBRD.compareTo(differentBRD),
                originalBRD.compareTo(differentBRD) != 0);
    }

    @Test
    public  void testHashCode(){
        assertTrue("Different hash codes for identical objects, object-a: "+
            originalBRD.hashCode() + " object-b: "+ identicalBRD.hashCode(),
                originalBRD.hashCode() == identicalBRD.hashCode());

        assertTrue("Identical hash codes for different objects, object-a: " +
                originalBRD.hashCode() + " object-b: " + differentBRD.hashCode(),
                originalBRD.hashCode() != differentBRD.hashCode());
    }

    @Test
    public void testEquals(){
        assertTrue(originalBRD.equals(identicalBRD));
        assertTrue(originalBRD.equals(sameBRD));
        assertFalse(originalBRD.equals(differentBRD));
        assertFalse(originalBRD.equals(null));
        assertFalse(originalBRD.equals("False"));
        originalBRD = new BaseRecordDescription("Original", null);
        assertFalse(originalBRD.equals(differentBRD));
        originalBRD = new BaseRecordDescription(null, differentRD);
        assertFalse(originalBRD.equals(differentBRD));
        originalBRD = new BaseRecordDescription("Original", null);
        differentBRD = new BaseRecordDescription("Different", null);
        assertFalse(originalBRD.equals(differentBRD));
    }
}
