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
package dk.statsbiblioteket.doms.integration.summa.parsing;

/**
 * This class defines a set of constants referring to elements which must be
 * declared in a configuration for creation of a
 * <code>DOMSReadableStorage</code> instance.
 * 
 *@author Thomas Skou Hansen &lt;tsh@statsbiblioteket.dk&gt;
 */
public class ConfigurationKeys {

    /**
     * Key for the list of base declarations.
     */
    public static final String ACCESSIBLE_COLLECTION_BASES = "accessibleCollectionBases";

    /**
     * Key for the base name/id element in a base declaration.
     */
    public static final String COLLECTION_BASE_ID = "collectionBaseID";

    /**
     * Key for the collection PID element in a base declaration.
     */
    public static final String COLLECTION_PID = "collectionPID";

    /**
     * Key for the content model entry object PID element in a base declaration.
     */
    public static final String COLLECTION_ENTRY_CONTENT_MODEL_PID = "collectionEntryContentModelPID";

    /**
     * Key for the view ID element in a base declaration.
     */
    public static final String VIEW_ID = "viewID";

    /**
     * Key for the DOMS user name element in the root configuration.
     */
    public static final String DOMS_USER_NAME = "DOMSUserName";

    /**
     * Key for the DOMS password element in the root configuration.
     */
    public static final String DOMS_PASSWORD = "DOMSPassword";

    /**
     * Key for the DOMS web service end-point element in the root configuration.
     */
    public static final String DOMS_API_WEBSERVICE_URL = "DOMSWebserviceURL";
    public static final String OBJECT_STATE = "Published";
}
