package com.courage.platform.crm.canal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by zhangyong on 2020/2/5.
 */
public class MainApplication {

    private final static Logger logger = LoggerFactory.getLogger(MainApplication.class);

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        logger.info("开始启动crm系统数据库搜索同步");
        final ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath:spring-app.xml");
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    applicationContext.close();
                } catch (Throwable e) {
                    logger.error("applicationContext close error:", e);
                }
            }
        }));
        logger.info("结束启动crm系统数据库搜索同步,耗时:" + (System.currentTimeMillis() - start) + "ms");
    }

}
