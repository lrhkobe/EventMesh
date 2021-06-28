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
package org.apache.eventmesh.runtime.trace;

import io.openmessaging.api.Message;
import org.apache.eventmesh.api.trace.LogPointType;
import org.apache.eventmesh.api.trace.TraceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.ServiceLoader;

public class Trace {
    private static final Logger logger = LoggerFactory.getLogger(Trace.class);
    private static TraceService traceService;

    public void init() throws Exception{
        traceService = getSpiTraceService();
        if (traceService == null) {
            logger.error("can't load the traceService plugin, please check.");
            throw new RuntimeException("doesn't load the traceService plugin, please check.");
        }
        traceService.init();
    }

    public void start() throws Exception{
        traceService.start();
    }

    public void shutdown() throws Exception{
        traceService.shutdown();
    }

    public static void uploadTraceLog(LogPointType logPointType, Message message, int code, String msg, Properties extProperties) throws Exception{
        traceService.uploadTraceLog(logPointType, message,code,msg,extProperties);
    }

    private TraceService getSpiTraceService() {
        ServiceLoader<TraceService> serviceLoader = ServiceLoader.load(TraceService.class);
        if (serviceLoader.iterator().hasNext()) {
            return serviceLoader.iterator().next();
        }
        return null;
    }
}
