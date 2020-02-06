package com.courage.platform.crm.canal.service;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 * Created by zhangyong on 2020/2/6.
 */
@Service
public class CanalService {

    private static Logger logger = LoggerFactory.getLogger(CanalService.class);

    @Value("${zk.servers.address}")
    private String zkServers;

    @Value("${destination}")
    private String destination;

    @Value("${username}")
    private String username;

    @Value("${password}")
    private String password;

    private CanalConnector clusterCanalConnector;

    private Thread executeThread;

    private boolean running = false;

    @PostConstruct

    public void start() {
        logger.info("begin to start...");
        this.clusterCanalConnector = CanalConnectors.newClusterConnector(zkServers, destination, username, password);
        this.executeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                process();
            }
        });
        this.running = true;
        this.executeThread.start();
    }

    public void process() {
        int batchSize = 5 * 1024;
        while (running) {
            try {
                MDC.put("destination", destination);
                clusterCanalConnector.connect();
                clusterCanalConnector.subscribe();
                while (running) {
                    Message message = clusterCanalConnector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    logger.info("batchId:" + batchId);
                    if (batchId == -1 || size == 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                        }
                    } else {
                        logger.info("dosomething");
                    }

                    if (batchId != -1) {
                        clusterCanalConnector.ack(batchId); // 提交确认
                        // connector.rollback(batchId); // 处理失败, 回滚数据
                    }
                }
            } catch (Exception e) {
                logger.error("process error!", e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    // ignore
                }
            } finally {
                clusterCanalConnector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    public void shutdown() {
        this.clusterCanalConnector.disconnect();
    }

}
