package dk.statsbiblioteket.doms.integration.summa;

import dk.statsbiblioteket.doms.central.RecordDescription;
import dk.statsbiblioteket.doms.client.*;
import org.w3c.dom.Document;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: eab
 * Date: 6/7/11
 * Time: 11:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class OfflineDOMSWSClient implements DomsWSClient {

        public long modificationTime;
        public OfflineDOMSWSClient(){
            modificationTime = 0;
        }

        @Override
        public long getModificationTime(String collectionPID, String viewID,
                                        String state){
            return modificationTime;
        }

        @Override
        public List<RecordDescription> getModifiedEntryObjects(String collectionPID, String viewID, long timeStamp, String objectState, long offsetIndex, long maxRecordCount) throws ServerOperationFailed {
            List<RecordDescription> recordDescriptionList = new ArrayList<RecordDescription>(1);
            RecordDescription recordDescription = new RecordDescription();
            recordDescription.setCollectionPid("doms_radioTVCollection");
            recordDescription.setDate(1l);
            recordDescription.setEntryContentModelPid("TestEntryContentModel");
            recordDescription.setPid("uuid:12345");
            recordDescription.setState("Published");
            recordDescriptionList.add(recordDescription);
            return recordDescriptionList;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getViewBundle(String entryObjectPID, String viewID){
            return viewID+":"+entryObjectPID;
        }

        @Override
        public void setObjectLabel(String objectPID, String objectLabel, String comment) throws ServerOperationFailed {
        }

        @Override
        public void login(URL domsWSAPIEndpoint, String userName, String password) {
        }

        @Override
        public void setCredentials(URL domsWSAPIEndpoint, String userName,
                               String password){}

        @Override
        public String createObjectFromTemplate(String templatePID, String comment) throws ServerOperationFailed {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String createObjectFromTemplate(String templatePID, List<String> oldIdentifiers, String comment) throws ServerOperationFailed {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String createFileObject(String templatePID, FileInfo fileInfo, String comment) throws ServerOperationFailed {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void addFileToFileObject(String fileObjectPID, FileInfo fileInfo, String comment) throws ServerOperationFailed {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getFileObjectPID(URL fileURL) throws NoObjectFound, ServerOperationFailed {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public List<String> getPidFromOldIdentifier(String oldIdentifier) throws NoObjectFound, ServerOperationFailed {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Document getDataStream(String objectPID, String datastreamID) throws ServerOperationFailed {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void updateDataStream(String objectPID, String dataStreamID, Document newDataStreamContents, String comment) throws ServerOperationFailed {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void addObjectRelation(Relation relation, String comment) throws ServerOperationFailed {
        }

        @Override
        public void removeObjectRelation(Relation relation, String comment) throws ServerOperationFailed {
        }

        @Override
        public List<Relation> listObjectRelations(String objectPID, String relationType) throws ServerOperationFailed {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void publishObjects(String comment, String... pidsToPublish) throws ServerOperationFailed {
        }

        @Override
        public void unpublishObjects(String comment, String... pidsToUnpublish) throws ServerOperationFailed {
        }

        @Override
        public void deleteObjects(String comment, String... pidsToDelete) throws ServerOperationFailed {
        }

    }