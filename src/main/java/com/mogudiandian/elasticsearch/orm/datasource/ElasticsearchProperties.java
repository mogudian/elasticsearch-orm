package com.mogudiandian.elasticsearch.orm.datasource;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ES的配置，抄的 spring-data-elasticsearch 源码
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@ConfigurationProperties(prefix = "elasticsearch.datasource")
@Getter
@Setter
public class ElasticsearchProperties {

    /**
     * Comma-separated list of the Elasticsearch instances to use.
     */
    private List<String> uris = new ArrayList<>(Collections.singletonList("http://localhost:9200"));

    /**
     * Credentials username.
     */
    private String username;

    /**
     * Credentials password.
     */
    private String password;

    /**
     * Connection timeout.
     */
    private Duration connectionTimeout = Duration.ofSeconds(1);

    /**
     * Read timeout.
     */
    private Duration readTimeout = Duration.ofSeconds(30);

}
