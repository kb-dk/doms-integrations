/*
 * $Id$
 * $Revision$
 * $Date$
 * $Author$
 *
 * The DOMS project.
 * Copyright (C) 2007-2010  The State and University Library
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package dk.statsbiblioteket.doms.integration.summa;

import java.net.URI;

/**
 * A <code>BaseDOMSConfiguration</code> instance contains the necessary
 * information for describing a Summa <code>base</code> in DOMS terms.
 * 
 * @author &lt;tsh@statsbiblioteket.dk&gt;
 */
public class BaseDOMSConfiguration {

    private final URI collectionPID;
    private final URI contentModelEntryObjectPID;
    private final String viewID;

    /**
     * Create a new <code>BaseDOMSConfiguration</code> instance which points out
     * a view of a specific content model entry object in a specific collection,
     * stored in the DOMS.
     * 
     * @param collectionPID
     *            PID of the collection i question.
     * @param contentModelEntryObjectPID
     *            The PID of a content model entry object in the specified
     *            collection.
     * @param viewID
     *            ID of a view of the content model entry object.
     */
    public BaseDOMSConfiguration(URI collectionPID,
            URI contentModelEntryObjectPID, String viewID) {
        this.collectionPID = collectionPID;
        this.contentModelEntryObjectPID = contentModelEntryObjectPID;
        this.viewID = viewID;
    }

    /**
     * Get the PID of the DOMS collection held by this instance.
     * 
     * @return the collectionPID PID of the collection.
     */
    public URI getCollectionPID() {
        return collectionPID;
    }

    /**
     * Get the PID of the content model entry object held by this instance. The
     * object is a part of the collection identified by the PID returned by
     * {@link #getCollectionPID()}.
     * 
     * @return the PID of the content model entry object.
     */
    public URI getContentModelEntryObjectPID() {
        return contentModelEntryObjectPID;
    }

    /**
     * Get the view ID held by this instance. This ID identifies a view of the
     * content model entry object specified by the PID returned by
     * {@link #getContentModelEntryObjectPID()}.
     * 
     * @return the ID of the view.
     */
    public String getViewID() {
        return viewID;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((collectionPID == null) ? 0 : collectionPID.hashCode());
        result = prime
                * result
                + ((contentModelEntryObjectPID == null) ? 0
                        : contentModelEntryObjectPID.hashCode());
        result = prime * result + ((viewID == null) ? 0 : viewID.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof BaseDOMSConfiguration)) {
            return false;
        }
        BaseDOMSConfiguration other = (BaseDOMSConfiguration) obj;
        if (collectionPID == null) {
            if (other.collectionPID != null) {
                return false;
            }
        } else if (!collectionPID.equals(other.collectionPID)) {
            return false;
        }
        if (contentModelEntryObjectPID == null) {
            if (other.contentModelEntryObjectPID != null) {
                return false;
            }
        } else if (!contentModelEntryObjectPID
                .equals(other.contentModelEntryObjectPID)) {
            return false;
        }
        if (viewID == null) {
            if (other.viewID != null) {
                return false;
            }
        } else if (!viewID.equals(other.viewID)) {
            return false;
        }
        return true;
    }
}
