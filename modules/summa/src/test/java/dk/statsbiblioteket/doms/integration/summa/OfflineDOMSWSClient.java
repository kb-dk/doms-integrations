package dk.statsbiblioteket.doms.integration.summa;

import dk.statsbiblioteket.doms.central.*;
import dk.statsbiblioteket.doms.client.*;
import dk.statsbiblioteket.doms.client.exceptions.NoObjectFound;
import dk.statsbiblioteket.doms.client.exceptions.ServerOperationFailed;
import dk.statsbiblioteket.doms.client.objects.FedoraState;
import dk.statsbiblioteket.doms.client.relations.LiteralRelation;
import dk.statsbiblioteket.doms.client.relations.Relation;
import dk.statsbiblioteket.doms.client.utils.FileInfo;
import org.w3c.dom.Document;

import java.io.InputStream;
import java.lang.String;
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
    public FedoraState getState(String pid) throws ServerOperationFailed {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getDatastreamContent(String pid, String ds)
            throws ServerOperationFailed, InvalidCredentialsException, MethodFailedException, InvalidResourceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void login(URL domsWSAPIEndpoint, String userName, String password) {
    }

    @Override
    public List<String> getLabel(List<String> uuids) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getLabel(String uuid) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<SearchResult> search(String query, int offset, int pageLength) throws ServerOperationFailed {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
    public void addObjectRelation(LiteralRelation relation, String comment) throws ServerOperationFailed {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeObjectRelation(LiteralRelation relation, String comment) throws ServerOperationFailed {
        //To change body of implemented methods use File | Settings | File Templates.
    }


    public void addObjectRelation(Relation relation, String comment) throws ServerOperationFailed {
    }

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