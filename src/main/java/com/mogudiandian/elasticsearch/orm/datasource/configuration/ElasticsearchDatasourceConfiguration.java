package com.mogudiandian.elasticsearch.orm.datasource.configuration;

import com.mogudiandian.elasticsearch.orm.datasource.ElasticsearchProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * ES数据源启动类
 * @author sunbo
 */
@Configuration
@EnableConfigurationProperties(ElasticsearchProperties.class)
public class ElasticsearchDatasourceConfiguration {

}