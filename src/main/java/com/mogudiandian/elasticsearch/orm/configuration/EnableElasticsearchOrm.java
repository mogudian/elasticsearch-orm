package com.mogudiandian.elasticsearch.orm.configuration;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * 启用TaskSearch功能
 * @author sunbo
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(ElasticsearchOrmAutoConfiguration.class)
@Documented
public @interface EnableElasticsearchOrm {

}
