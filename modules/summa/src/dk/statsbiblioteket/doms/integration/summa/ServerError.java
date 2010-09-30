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
 * Exception to be thrown if any problems were encountered while communicating
 * with the DOMS server.
 * 
 * <p/>
 * TODO: This class is a copy from the radio-tv example ingester project. It
 * should be moved into some utility library some time!
 * <p/>
 * 
 * TODO! Rename this class; ServerError is a pretty bad name for an exception...
 * <p/>
 * 
 *@author Thomas Skou Hansen &lt;tsh@statsbiblioteket.dk&gt;
 */
public class ServerError extends Exception {

    private static final long serialVersionUID = -5490820003022778643L;

    public ServerError() {
    }

    /**
     * @param message
     *            a message containing details on why this exception was thrown.
     */
    public ServerError(String message) {
        super(message);
    }

    /**
     * @param cause
     *            the exception that caused this exception to be thrown.
     */
    public ServerError(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     *            a message containing details on why this exception was thrown.
     * @param cause
     *            the exception that caused this exception to be thrown.
     */
    public ServerError(String message, Throwable cause) {
        super(message, cause);
    }
}
