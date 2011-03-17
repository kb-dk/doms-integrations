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


import dk.statsbiblioteket.doms.central.RecordDescription;

/**
 * @author Thomas Skou Hansen &lt;tsh@statsbiblioteket.dk&gt;
 */
class BaseRecordDescription implements Comparable<BaseRecordDescription> {

    private final RecordDescription recordDescription;
    private final String summaBaseID;

    /**
     * 
     */
    public BaseRecordDescription(String summaBaseID,
            RecordDescription recordDescription) {
        this.summaBaseID = summaBaseID;
        this.recordDescription = recordDescription;
    }

    /**
     * @return the recordDescription
     */
    public RecordDescription getRecordDescription() {
        return recordDescription;
    }

    /**
     * @return the summaBaseID
     */
    public String getSummaBaseID() {
        return summaBaseID;
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(BaseRecordDescription other) {
        long timeDiff = getRecordDescription().getDate()
                - other.getRecordDescription().getDate();
        if (timeDiff != 0) {
            return (timeDiff < 0) ? -1 : 1;
        } else {
            return getSummaBaseID().compareTo(other.getSummaBaseID());
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((recordDescription == null) ? 0 : recordDescription
                        .hashCode());
        result = prime * result
                + ((summaBaseID == null) ? 0 : summaBaseID.hashCode());
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
        if (!(obj instanceof BaseRecordDescription)) {
            return false;
        }
        BaseRecordDescription other = (BaseRecordDescription) obj;
        if (recordDescription == null) {
            if (other.recordDescription != null) {
                return false;
            }
        } else if (!recordDescription.equals(other.recordDescription)) {
            return false;
        }
        if (summaBaseID == null) {
            if (other.summaBaseID != null) {
                return false;
            }
        } else if (!summaBaseID.equals(other.summaBaseID)) {
            return false;
        }
        return true;
    }
}
