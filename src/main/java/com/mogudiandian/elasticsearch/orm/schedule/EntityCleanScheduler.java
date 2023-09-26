package com.mogudiandian.elasticsearch.orm.schedule;

import com.mogudiandian.elasticsearch.orm.datasource.ElasticsearchDatasourceDelegator;
import com.mogudiandian.elasticsearch.orm.EntityCleaner;
import com.mogudiandian.elasticsearch.orm.configuration.ElasticsearchOrmProperties;
import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 实体清理的定时任务
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Slf4j
@Component
public class EntityCleanScheduler {

    @Autowired
    private ElasticsearchOrmProperties properties;

    @Autowired
    private EntityCleaner cleaner;

    @Autowired
    private ElasticsearchDatasourceDelegator elasticSearchDatasourceDelegator;

    @Autowired(required = false)
    private EntityCleanLocker locker;

    /**
     * 时间可设置 默认为每天 03:02:01 跑任务
     */
    @Scheduled(cron = "${es.orm.cleaner.cron:1 2 3 * * *}")
    public void schedule() {
        if (!properties.isEnableCleanScheduler()) {
            return;
        }
        lockAndClean();
    }

    /**
     * 清理入口 主要是 加锁-清理-解锁
     */
    public void lockAndClean() {
        try {
            if (tryLock()) {
                OrmUtils.getAllEntityClasses()
                        .forEach(cleaner::clean);
            }
        } catch (Exception e) {
            log.error("cleaner throws some exception when it clean, exception is: ", e);
        } finally {
            unlock();
        }
    }

    private boolean tryLock() {
        if (properties.isEnableCleanLock() && locker == null) {
            DefaultEntityCleanLocker locker = new DefaultEntityCleanLocker();
            locker.setElasticSearchDatasourceDelegator(elasticSearchDatasourceDelegator);
            this.locker = locker;
        }
        if (locker != null) {
            return locker.tryLock();
        }
        return true;
    }

    private void unlock() {
        if (locker != null) {
            locker.unlock();
        }
    }

}
