/*
 * Copyright (c) 2019 the Eclipse Milo Authors
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package org.eclipse.milo.examples.client;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Lists.newArrayList;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

public class SubscriptionExample implements ClientExample {

    private static HikariDataSource dataSource = null;
    private AtomicInteger itemCount = new AtomicInteger(0);
    private LocalDateTime startTime ;
    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss.SSS");

//    private static List<ItemVO> itemVOS = new CopyOnWriteArrayList<>();
    private ConcurrentLinkedQueue<ItemVO> itemVOConcurrentLinkedQueue = new ConcurrentLinkedQueue<>();

    public static class ItemVO {
        private UaMonitoredItem item;
        private DataValue value;
        ItemVO(UaMonitoredItem item, DataValue value){
            this.item = item;
            this.value = value;
        }

        public UaMonitoredItem getItem() {
            return item;
        }

        public DataValue getValue() {
            return value;
        }

    }

    public static void main(String[] args) throws Exception {

        HikariConfig config = new HikariConfig();
        // jdbc properties
        config.setJdbcUrl("jdbc:TAOS://localhost:6030/opc");
        config.setUsername("root");
        config.setPassword("taosdata");
        // connection pool configurations
        config.setMinimumIdle(10);           //minimum number of idle connection
        config.setMaximumPoolSize(10);      //maximum number of connection in the pool
        config.setConnectionTimeout(30000); //maximum wait milliseconds for get connection from pool
        config.setMaxLifetime(0);       // maximum life time for each connection
        config.setIdleTimeout(0);       // max idle time for recycle idle connection
        config.setConnectionTestQuery("select server_status()"); //validation query

        SubscriptionExample.dataSource = new HikariDataSource(config); //create datasource

        dataSource.getConnection();
        SubscriptionExample example = new SubscriptionExample();

        new ClientExampleRunner(example).run();
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void run(OpcUaClient client, CompletableFuture<OpcUaClient> future) throws Exception {
        // synchronous connect
        client.connect().get();

        // create a subscription @ 1000ms
        UaSubscription subscription = client.getSubscriptionManager().createSubscription(10000.0).get();

        List<MonitoredItemCreateRequest> requests = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 1000; j++) {

                // subscribe to the Value attribute of the server's CurrentTime node
                ReadValueId readValueId = new ReadValueId(
                        new NodeId(2,"HelloWorld/Dynamic_folder_"+i+"/Double"+j),
//                        Identifiers.Double,
                        AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE
                );

                // IMPORTANT: client handle must be unique per item within the context of a subscription.
                // You are not required to use the UaSubscription's client handle sequence; it is provided as a convenience.
                // Your application is free to assign client handles by whatever means necessary.
                UInteger clientHandle = subscription.nextClientHandle();

                MonitoringParameters parameters = new MonitoringParameters(
                        clientHandle,
                        1000.0,     // sampling interval
                        null,       // filter, null means use default
                        uint(10),   // queue size
                        true        // discard oldest
                );
                MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(
                        readValueId,
                        MonitoringMode.Reporting,
                        parameters
                );
//                requests.add(request);
                List<UaMonitoredItem> items = subscription.createMonitoredItems(
                        TimestampsToReturn.Both,
                        newArrayList(request),
                        ((item, index) -> item.setValueConsumer(this::addItem))
                ).get();

                for (UaMonitoredItem item : items) {
                    if (item.getStatusCode().isGood()) {
                        logger.info("item created for nodeId={}", item.getReadValueId().getNodeId());
                    } else {
                        logger.warn(
                                "failed to create item for nodeId={} (status={})",
                                item.getReadValueId().getNodeId(), item.getStatusCode());
                    }
                }
            }
        }

        logger.info("开始启动写入数据线程");
        startTime = LocalDateTime.now();
        for (int n = 0; n < 12; n++) {
            Thread threadDB = new Thread(new Runnable() {

                @Override
                public void run() {
//                    String sql = "INSERT INTO HelloWorld_Dynamic_folder_96_Double701 USING test TAGS('good','HelloWorld/Dynamic_folder_96/Double701') VALUES('2022-09-21 22:16:10.000',0.615,'2022-09-21 22:16:10.000');";
                    List<ItemVO> itemVOS = new ArrayList<>();
                    int cycles = 0;
                    while (true) {
                        long time = System.currentTimeMillis();
                        cycles+=1;
                        if(itemVOConcurrentLinkedQueue.size()>0){
                            ItemVO itemVO = null;
                            do{
                                itemVO = itemVOConcurrentLinkedQueue.poll();
                                if(itemVO!=null) itemVOS.add(itemVO);
                                else break;
                            }while (itemVOS.size()<500);
                            if(itemVOS.size()>=500 || (cycles>3 && itemVOS.size()>0)){
                                writeDB(itemVOS);
                                itemVOS.clear();
                                cycles=0;
                            }
                            long writeDbTime = System.currentTimeMillis() - time;
                            try {
                                Thread.sleep(Math.max(50-writeDbTime,0));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }else{
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }

            });
            threadDB.start();
            logger.info("写入数据线程启动");
        }
        // let the example run for 5 seconds then terminate
        Thread.sleep(Long.MAX_VALUE);
        future.complete(client);
    }

    private void writeDB(List<ItemVO> itemVOS){
        String sql = "INSERT INTO";
        String sqlFormat = " '%s' USING test TAGS ('%s','%s') VALUES (%d,%f,%d)";
        for (ItemVO itemVO : itemVOS) {
            UaMonitoredItem item = itemVO.getItem();
            DataValue dataValue = itemVO.getValue();
            String nodeId = ((String) item.getReadValueId().getNodeId().getIdentifier()).replace("/", "_");
            sql = sql + String.format(sqlFormat, nodeId,
                    item.getStatusCode().isBad() ? "bad" : (item.getStatusCode().isGood() ? "good" : "uncertain"),
                    nodeId, dataValue.getServerTime().getJavaTime(),
                    dataValue.getValue().getValue(), dataValue.getSourceTime().getJavaTime());
        }
        long time = System.currentTimeMillis();
        int result = 0;
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            result = statement.executeUpdate(sql);
            connection.close();
        }catch (SQLException exception) {
            exception.printStackTrace();
        }
        itemCount.addAndGet(result);
        long writeDbTime = System.currentTimeMillis() - time;
        long durationTime = Duration.between(startTime,LocalDateTime.now()).getSeconds();
        durationTime = durationTime<=0?1:durationTime;
        logger.info("插入开始时间 {} 当前时间 {} 总条数 {} 平均每秒写入 {} 插入TDEngine条数 {}, 耗时 {} ms, 剩余条数 {}", timeFormatter.format(startTime),
                timeFormatter.format(LocalDateTime.now()),itemCount.get(),
                itemCount.get()*1.000/durationTime,
                result, writeDbTime,itemVOConcurrentLinkedQueue.size());
    }
    private void addItem(UaMonitoredItem item, DataValue value){
        itemVOConcurrentLinkedQueue.add(new ItemVO(item,value));
//        logger.info("接收到数据变动 列表总条数 {}",itemVOS.size());
    }

}
