package com.mogudiandian.elasticsearch.orm.configuration;

import com.mogudiandian.elasticsearch.orm.datasource.ElasticsearchDatasource;
import com.mogudiandian.elasticsearch.orm.datasource.ElasticsearchDatasourceDelegator;
import com.mogudiandian.elasticsearch.orm.datasource.SingleElasticsearchDatasourceDelegator;
import com.mogudiandian.elasticsearch.orm.DefaultEntityCleaner;
import com.mogudiandian.elasticsearch.orm.DefaultElasticsearchOrmClient;
import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;
import com.mogudiandian.elasticsearch.orm.datasource.configuration.ElasticsearchDatasourceConfiguration;
import com.mogudiandian.elasticsearch.orm.schedule.EntityCleanScheduler;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;

/**
 * ES-ORM配置启动类
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Configuration
@EnableConfigurationProperties(ElasticsearchOrmProperties.class)
@ConditionalOnClass(RestHighLevelClient.class)
@Import({ElasticsearchDatasourceConfiguration.class, DefaultElasticsearchOrmClient.class, DefaultEntityCleaner.class, EntityCleanScheduler.class})
@AutoConfigureAfter(ElasticsearchDatasourceConfiguration.class)
@EnableScheduling
public class ElasticsearchOrmAutoConfiguration {

    @Autowired
    private ElasticsearchOrmProperties properties;

    @Bean
    @ConditionalOnClass(ElasticsearchDatasource.class)
    @ConditionalOnMissingBean(ElasticsearchDatasource.class)
    public ElasticsearchDatasource elasticSearchDatasource() {
        return new ElasticsearchDatasource(properties);
    }

    @Bean
    @ConditionalOnClass(ElasticsearchDatasourceDelegator.class)
    @ConditionalOnMissingBean(ElasticsearchDatasourceDelegator.class)
    public ElasticsearchDatasourceDelegator elasticSearchDatasourceDelegator(ElasticsearchDatasource elasticSearchDatasource) {
        return new SingleElasticsearchDatasourceDelegator(elasticSearchDatasource);
    }

    @PostConstruct
    public void init() {
        OrmUtils.init(properties.getBasePackage());
    }

}