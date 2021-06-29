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
package org.apache.eventmesh.runtime.connector;

import org.apache.eventmesh.api.connector.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

public class ConnectorResource {

    private static final Logger logger = LoggerFactory.getLogger(ConnectorResource.class);
    private static ConnectorService connectorService;

    public void init() throws Exception{
        connectorService = getSpiConnectorService();
        if (connectorService == null) {
            logger.error("can't load the connectorService plugin, please check.");
            throw new RuntimeException("doesn't load the connectorService plugin, please check.");
        }
        connectorService.init();
    }

    public void close()throws Exception{
        connectorService.close();
    }

    private ConnectorService getSpiConnectorService() {
        ServiceLoader<ConnectorService> serviceLoader = ServiceLoader.load(ConnectorService.class);
        if (serviceLoader.iterator().hasNext()) {
            return serviceLoader.iterator().next();
        }
        return null;
    }

}
