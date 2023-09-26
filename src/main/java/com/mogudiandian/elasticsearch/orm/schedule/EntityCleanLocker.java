package com.mogudiandian.elasticsearch.orm.schedule;

/**
 * 实体清理的定时任务依赖的锁
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public interface EntityCleanLocker {

    /**
     * 加锁
     * @return true表示加锁成功 false表示加锁失败
     */
    boolean tryLock();

    /**
     * 解锁
     */
    void unlock();

}
