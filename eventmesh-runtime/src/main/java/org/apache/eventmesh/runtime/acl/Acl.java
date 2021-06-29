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
package org.apache.eventmesh.runtime.acl;

import org.apache.eventmesh.api.acl.AclService;
import org.apache.eventmesh.api.exception.AclException;
import org.apache.eventmesh.common.protocol.tcp.UserAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

public class Acl {
    private static final Logger logger = LoggerFactory.getLogger(Acl.class);
    private static AclService aclService;

    public void init() throws AclException{
        aclService = getSpiAclService();
        if (aclService == null) {
            logger.error("can't load the aclService plugin, please check.");
            throw new RuntimeException("doesn't load the aclService plugin, please check.");
        }
        aclService.init();
    }

    public void start() throws AclException{
        aclService.start();
    }

    public void shutdown() throws AclException{
        aclService.shutdown();
    }

    public static void doAclCheckInConnect(String remoteAddr, UserAgent userAgent, int requestCode) throws AclException{
        aclService.doAclCheckInConnect(remoteAddr, userAgent, requestCode);
    }

    public static void doAclCheckInHeartbeat(String remoteAddr, UserAgent userAgent, int requestCode) throws AclException{
        aclService.doAclCheckInHeartbeat(remoteAddr, userAgent, requestCode);
    }

    public static void doAclCheckInSend(String remoteAddr, UserAgent userAgent, String topic, int requestCode) throws AclException{
        aclService.doAclCheckInSend(remoteAddr, userAgent, topic, requestCode);
    }

    public static void doAclCheckInReceive(String remoteAddr, UserAgent userAgent, String topic, int requestCode) throws AclException{
        aclService.doAclCheckInReceive(remoteAddr, userAgent, topic, requestCode);
    }

    public static void doAclCheckInHttpSend(String remoteAddr, String user, String pass,String subsystem, String topic, int requestCode) throws AclException{
        aclService.doAclCheckInHttpSend(remoteAddr, user, pass, subsystem, topic, requestCode);
    }

    public static void doAclCheckInHttpReceive(String remoteAddr, String user, String pass,String subsystem, String topic, int requestCode) throws AclException{
        aclService.doAclCheckInHttpReceive(remoteAddr, user, pass, subsystem, topic, requestCode);
    }

    public static void doAclCheckInHttpHeartbeat(String remoteAddr, String user, String pass,String subsystem, String topic, int requestCode) throws AclException{
        aclService.doAclCheckInHttpHeartbeat(remoteAddr, user, pass, subsystem, topic, requestCode);
    }

    private AclService getSpiAclService() {
        ServiceLoader<AclService> serviceLoader = ServiceLoader.load(AclService.class);
        if (serviceLoader.iterator().hasNext()) {
            return serviceLoader.iterator().next();
        }
        return null;
    }

}
