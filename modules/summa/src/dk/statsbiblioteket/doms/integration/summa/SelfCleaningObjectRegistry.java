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

import dk.statsbiblioteket.util.caching.TimeSensitiveCache;

/**
 * @author Thomas Skou Hansen &lt;tsh@statsbiblioteket.dk&gt;
 */
public class SelfCleaningObjectRegistry<T> {

    /**
     * This container associates objects with the keys generated and returned by
     * this <code>SelfCleaningObjectRegistry</code>. The objects and their
     * associated key will automatically be removed from the container if they
     * are not accessed within the expiration time specified when calling the
     * constructor.
     */
    private final TimeSensitiveCache<Long, T> objectsByKey;

    /**
     * Create a <code>SelfCleaningObjectRepository</code> instance which will
     * delete registered keys and objects if they are not accessed within the
     * expiration time specified by <code>objectExpirationTimeMillis</code>.
     * 
     * @param objectExpirationTimeMillis
     *            the object expiration time in milliseconds.
     */
    public SelfCleaningObjectRegistry(long objectExpirationTimeMillis) {

        objectsByKey = new TimeSensitiveCache<Long, T>(
                objectExpirationTimeMillis, true);
    }

    /**
     * Get the object associated with <code>key</code>. This method will throw
     * an exception if <code>key</code> is unknown or if the associated object
     * has expired. That is, if it has not been accessed within the expiration
     * time specified at construction time of this registry.
     * 
     * @param key
     *            The key associated with the object to fetch.
     * @return the object associated with <code>key</code>.
     * @throws UnknownKeyException
     *             if <code>key</code> is unknown to this registry. This may
     *             happen e.g. if the requested object has expired.
     */
    public T get(long key) throws UnknownKeyException {

        final T object = objectsByKey.get(key);
        if (object == null) {
            throw new UnknownKeyException("Unknown key = " + key
                    + " Has the requested object expired?");
        }
        return object;
    }

    /**
     * Remove the object associated with <code>key</code> from this registry and
     * make the key available for association with other objects.
     * 
     * @param key
     *            The key associated with the object to remove.
     * @throws UnknownKeyException
     *             if <code>key</code> is unknown to this registry. This may
     *             happen e.g. if the requested object has expired.
     */
    public void remove(long key) throws UnknownKeyException {
        T object = objectsByKey.remove(key);
        if (object == null) {
            throw new UnknownKeyException("Unknown key = " + key
                    + " Has the key expired?");
        }
    }

    /**
     * Register <code>object</code> in the internal container and return the
     * iterator key.
     * <p/>
     * This method will attempt to generate a random, non-negative key which is
     * not already used by the internal container. However, if this fails it
     * will throw an exception.
     * 
     * @param object
     *            an object to add to the internal container.
     * @return the key associated with the object.
     * @throws RegistryFullException
     *             if the registry has no vacant keys.
     */
    public synchronized long register(T object) throws RegistryFullException {

        long key = Math.round(Long.MAX_VALUE * Math.random());
        long emergencyBrake = 0;

        while (objectsByKey.containsKey(key)) {
            key = Math.round(Long.MAX_VALUE * Math.random());
            emergencyBrake++;
            if (emergencyBrake == 0) {
                throw new RegistryFullException("Unable to produce an iterator"
                        + " key for the object: " + object);
            }
        }

        objectsByKey.put(key, object);

        return key;
    }
}
