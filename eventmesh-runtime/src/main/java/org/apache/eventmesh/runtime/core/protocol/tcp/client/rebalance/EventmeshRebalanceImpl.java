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
package org.apache.eventmesh.runtime.core.protocol.tcp.client.rebalance;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.eventmesh.api.registry.dto.EventMeshClientDistributionMapData;
import org.apache.eventmesh.api.registry.dto.EventMeshInfoData;
import org.apache.eventmesh.api.registry.dto.EventMeshInfoList;
import org.apache.eventmesh.runtime.boot.EventMeshTCPServer;
import org.apache.eventmesh.runtime.constants.EventMeshConstants;
import org.apache.eventmesh.runtime.core.protocol.tcp.client.EventMeshTcp2Client;
import org.apache.eventmesh.runtime.core.protocol.tcp.client.recommend.EventMeshRecommendImpl;
import org.apache.eventmesh.runtime.core.protocol.tcp.client.recommend.EventMeshRecommendStrategy;
import org.apache.eventmesh.runtime.core.protocol.tcp.client.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EventmeshRebalanceImpl implements EventMeshRebalanceStrategy {

    protected final Logger logger = LoggerFactory.getLogger(EventmeshRebalanceImpl.class);

    private EventMeshTCPServer eventMeshTCPServer;

    public EventmeshRebalanceImpl(EventMeshTCPServer eventMeshTCPServer){
        this.eventMeshTCPServer = eventMeshTCPServer;
    }

    @Override
    public void doRebalance() throws Exception {
        long startTime = System.currentTimeMillis();
        logger.info("doRebalance start===========startTime:{}",startTime);

        Set<String> groupSet = eventMeshTCPServer.getClientSessionGroupMapping().getClientGroupMap().keySet();
        if(CollectionUtils.isEmpty(groupSet)){
            logger.warn("doRebalance failed,eventmesh has no group, please check eventmeshData");
            return;
        }

        final String cluster = eventMeshTCPServer.getEventMeshTCPConfiguration().eventMeshCluster;
        //get eventmesh of local idc
        Map<String, String> localEventMeshMap = queryLocalEventMeshMap(cluster);
        if(localEventMeshMap == null || localEventMeshMap.size() == 0){
            return;
        }

        for(String group : groupSet){
            doRebalanceByGroup(cluster, group, EventMeshConstants.PURPOSE_SUB, localEventMeshMap);
        }
        logger.info("doRebalance end===========startTime:{}, cost:{}", startTime, System.currentTimeMillis() - startTime);
    }

    private Map<String, String> queryLocalEventMeshMap(String cluster){
        Map<String, String> localEventMeshMap = null;
        EventMeshInfoList eventMeshInfoList = null;
        try{
            eventMeshInfoList = eventMeshTCPServer.getRegistry().findEventMeshInfoByCluster(cluster);

            if(eventMeshInfoList == null || CollectionUtils.isEmpty(eventMeshInfoList.getEventMeshInfoDataList())){
                logger.warn("doRebalance failed,query eventmesh instances is null from registry,cluster:{}", cluster);
                return null;
            }
            localEventMeshMap = new HashMap<>();
            String localIdc = eventMeshTCPServer.getEventMeshTCPConfiguration().eventMeshIDC;
            for(EventMeshInfoData eventMeshInfoData : eventMeshInfoList.getEventMeshInfoDataList()){
                String idc = eventMeshInfoData.getEventMeshName().split("-")[0];
                if(StringUtils.isNotBlank(idc) && StringUtils.equals(idc, localIdc)){
                    localEventMeshMap.put(eventMeshInfoData.getEventMeshName(), eventMeshInfoData.getEndpoint());
                }
            }

            if(0 == localEventMeshMap.size()){
                logger.warn("doRebalance failed,query eventmesh instances of localIDC is null from registry,localIDC:{},cluster:{}", localIdc,cluster);
                return null;
            }
        }catch (Exception e){
            logger.warn("doRebalance failed,findEventMeshInfoByCluster failed,cluster:{},errMsg:{}", cluster, e);
            return null;
        }

        return localEventMeshMap;
    }

    private void doRebalanceByGroup(String cluster, String group, String purpose, Map<String, String> eventMeshMap) throws Exception{
        //query distribute data of loacl idc
        Map<String, Integer> clientDistributionMap = queryLocalEventMeshDistributeData(cluster, group, purpose, eventMeshMap);
        if(clientDistributionMap == null || clientDistributionMap.size() == 0){
            return;
        }

        int sum = 0;
        for(Integer item : clientDistributionMap.values()){
            sum += item.intValue();
        }
        int currentNum = clientDistributionMap.get(eventMeshTCPServer.getEventMeshTCPConfiguration().eventMeshName);
        int avgNum = sum / clientDistributionMap.size();
        int judge = avgNum >= 2 ? avgNum/2 : 1;

        // 排序后，只有当前proxy上该组实例数比proxy上最少实例数 大于judge时，才需要进行负载均衡
        if(currentNum - avgNum > judge) {
            Set<Session> sessionSet = eventMeshTCPServer.getClientSessionGroupMapping().getClientGroupMap().get(group).getGroupConsumerSessions();
            List<Session> sessionList = new ArrayList<>(sessionSet);
            Collections.shuffle(new ArrayList<>(sessionList));
            EventMeshRecommendStrategy eventMeshRecommendStrategy = new EventMeshRecommendImpl(eventMeshTCPServer);
            List<String> eventMeshRecommendResult = eventMeshRecommendStrategy.calculateRedirectRecommendEventMesh(eventMeshMap, clientDistributionMap, group, judge);
            if(eventMeshRecommendResult == null || eventMeshRecommendResult.size() != judge){
                logger.warn("doRebalance failed,recommendProxyNum is not consistent,recommendResult:{},judge:{}", eventMeshRecommendResult, judge);
                return;
            }
            logger.info("doRebalance redirect start---------------------group:{},judge:{}", group, judge);
            for(int i= 0; i<judge; i++){
                //String redirectSessionAddr = ProxyTcp2Client.redirectClientForRebalance(sessionList.get(i), eventMeshTCPServer.getClientSessionGroupMapping());
                String newProxyIp = eventMeshRecommendResult.get(i).split(":")[0];
                String newProxyPort = eventMeshRecommendResult.get(i).split(":")[1];
                String redirectSessionAddr = EventMeshTcp2Client.redirectClient2NewEventMesh(eventMeshTCPServer,newProxyIp,Integer.valueOf(newProxyPort),sessionList.get(i), eventMeshTCPServer.getClientSessionGroupMapping());
                logger.info("doRebalance,redirect sessionAddr:{}", redirectSessionAddr);
            }
            logger.info("doRebalance redirect end---------------------group:{}", group);
        }else{
            logger.info("rebalance condition not satisfy,group:{},sum:{},currentNum:{},avgNum:{},judge:{}", group, sum, currentNum, avgNum, judge);
        }
    }

    private Map<String, Integer> queryLocalEventMeshDistributeData(String cluster, String group, String purpose, Map<String, String> eventMeshMap){
        Map<String, Integer> localEventMeshDistributeData = null;
        EventMeshClientDistributionMapData eventMeshClientDistributionMapData = null;
        try{
            eventMeshClientDistributionMapData = eventMeshTCPServer.getRegistry().findEventMeshClientDistributionData(cluster, group, purpose);

            if(eventMeshClientDistributionMapData == null ||
                    eventMeshClientDistributionMapData.getClientDistributionMap() == null ||
                    eventMeshClientDistributionMapData.getClientDistributionMap().size() == 0){

                logger.warn("doRebalance failed,found no distribute data in regitry, cluster:{}, group:{}", cluster, group);
                return null;
            }

            localEventMeshDistributeData = new HashMap<>();
            String localIdc = eventMeshTCPServer.getEventMeshTCPConfiguration().eventMeshIDC;
            for(Map.Entry<String, Map<String, Integer>> entry : eventMeshClientDistributionMapData.getClientDistributionMap().entrySet()){
                String idc = entry.getKey().split("-")[0];
                if(StringUtils.isNotBlank(idc) && StringUtils.equals(idc, localIdc)){
                    localEventMeshDistributeData.put(entry.getKey(), entry.getValue().get(purpose));
                }
            }

            if(0 == localEventMeshDistributeData.size()){
                logger.warn("doRebalance failed,found no distribute data of localIDC in regitry,cluster:{},group:{},localIDC:{}", cluster, group,localIdc);
                return null;
            }

            logger.info("before revert clientDistributionMap:{}, group:{}", localEventMeshDistributeData, group);
            for(String eventMeshName : localEventMeshDistributeData.keySet()){
                if(!eventMeshMap.keySet().contains(eventMeshName)){
                    logger.warn("doRebalance failed,exist eventMesh not register but exist in distributionMap,cluster:{},grpup:{},eventMeshName:{}", cluster, group, eventMeshName);
                    return null;
                }
            }
            for(String eventMesh : eventMeshMap.keySet()){
                if(!localEventMeshDistributeData.keySet().contains(eventMesh)){
                    localEventMeshDistributeData.put(eventMesh, 0);
                }
            }
            logger.info("after revert clientDistributionMap:{}, group:{}", localEventMeshDistributeData, group);
        }catch (Exception e){
            logger.warn("doRebalance failed,cluster:{},group:{},findProxyClientDistributionData failed, errMsg:{}", cluster, group, e);
            return null;
        }

        return localEventMeshDistributeData;
    }


    private class ValueComparator implements Comparator<Map.Entry<String, Integer>> {
        @Override
        public int compare(Map.Entry<String, Integer> x, Map.Entry<String, Integer> y) {
            return x.getValue().intValue() - y.getValue().intValue();
        }
    }
}
