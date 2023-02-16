package com.mogudiandian.elasticsearch.orm.configuration;

import com.mogudiandian.elasticsearch.orm.datasource.ElasticsearchProperties;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 任务搜索的配置集合
 * @author sunbo
 */
@ConfigurationProperties(prefix = "elasticsearch.orm")
@Getter
@Setter
public class ElasticsearchOrmProperties extends ElasticsearchProperties {

    /**
     * 扫描的包
     */
    private String basePackage;

    /**
     * 默认的时间格式
     */
    private String defaultDateFormat = "yyyy-MM-dd HH:mm:ss";

    /**
     * ES更新的超时时间
     */
    private Long updateTimeout = 10000L;

    /**
     * ES查询的超时时间
     */
    private Long queryTimeout = 5000L;

    /**
     * 启用清理任务
     */
    private boolean enableCleanScheduler = true;

    /**
     * 清理是否需要加锁
     */
    private boolean enableCleanLock = true;

}
