/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eventmesh.runtime.registry;

import org.apache.eventmesh.api.registry.RegistryService;
import org.apache.eventmesh.api.registry.dto.*;
import org.apache.eventmesh.runtime.acl.Acl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

public class Registry {
    private static final Logger logger = LoggerFactory.getLogger(Acl.class);
    private static RegistryService registryService;

    public void init() throws Exception {
        registryService = getSpiRegistryService();
        if (registryService == null) {
            logger.error("can't load the registryService plugin, please check.");
            throw new RuntimeException("doesn't load the registryService plugin, please check.");
        }
        registryService.init();
    }

    public void start() throws Exception{
        registryService.start();
    }

    public void shutdown() throws Exception{
        registryService.shutdown();
    }

    public EventMeshInfoList findEventMeshInfoByCluster(String clusterName) throws Exception{
        return registryService.findEventMeshInfoByCluster(clusterName);
    }

    public EventMeshClientDistributionMapData findEventMeshClientDistributionData(String clusterName, String group, String purpose) throws Exception{
        return registryService.findEventMeshClientDistributionData(clusterName, group, purpose);
    }

    public boolean register(EventMeshRegisterDTO eventMeshRegisterDTO) throws Exception{
        return registryService.register(eventMeshRegisterDTO);
    }

    public boolean unRegister(EventMeshUnRegisterDTO eventMeshUnRegisterDTO) throws Exception{
        return registryService.unRegister(eventMeshUnRegisterDTO);
    }

    private RegistryService getSpiRegistryService() {
        ServiceLoader<RegistryService> serviceLoader = ServiceLoader.load(RegistryService.class);
        if (serviceLoader.iterator().hasNext()) {
            return serviceLoader.iterator().next();
        }
        return null;
    }
}
