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

/**
 * This class tracks how far we have come in getting records from a Summa Base
 * @author Thomas Skou Hansen &lt;tsh@statsbiblioteket.dk&gt;
 */
public class BaseState {

    private long nextStartTime;

    private long currentRecordDescriptionCount;

    private boolean reachedEnd;


    BaseState() {
        setNextStartTime(-1);
        setCurrentRecordDescriptionCount(0);
        reachedEnd = false;
    }

    /**
     * This is the start time to be used in the next update tracker query
     * @param nextStartTime
     *            the nextStartTime to set
     */
    public void setNextStartTime(long nextStartTime) {
        this.nextStartTime = nextStartTime;
    }

    /**
     * @return the nextStartTime
     */
    public long getNextStartTime() {
        return nextStartTime;
    }

    /**
     * This is the number of records left in the current "retrieval", ie. how many we have before we must
     * query the update tracker again
     * @param currentRecordDescriptionCount
     *            the currentRecordDescriptionCount to set
     */
    public void setCurrentRecordDescriptionCount(
            long currentRecordDescriptionCount) {
        this.currentRecordDescriptionCount = currentRecordDescriptionCount;
    }

    /**
     * @return the currentRecordDescriptionCount
     */
    public long getCurrentRecordDescriptionCount() {
        return currentRecordDescriptionCount;
    }

    /**
     * This boolean tracks if we have reached the end of records from the base, ie. if we should
     * query doms/updatetracker for any more
     * @return
     */
    public boolean isReachedEnd() {
        return reachedEnd;
    }

    public void setReachedEnd(boolean reachedEnd) {
        this.reachedEnd = reachedEnd;
    }
}
