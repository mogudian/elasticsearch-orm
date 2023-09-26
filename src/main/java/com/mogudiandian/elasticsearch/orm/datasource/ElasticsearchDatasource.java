package com.mogudiandian.elasticsearch.orm.datasource;

import lombok.Getter;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.autoconfigure.elasticsearch.RestClientBuilderCustomizer;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * ES数据源
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Getter
public class ElasticsearchDatasource extends RestHighLevelClient {

    public ElasticsearchDatasource(ElasticsearchProperties properties) {
        super(defaultRestClientBuilder(properties));
    }

    /**
     * 根据配置生成builder
     * @param properties 配置
     * @return builder
     */
    private static RestClientBuilder defaultRestClientBuilder(ElasticsearchProperties properties) {
        HttpHost[] hosts = properties.getUris()
                                     .stream()
                                     .map(ElasticsearchDatasource::createHttpHost)
                                     .toArray(HttpHost[]::new);
        RestClientBuilder restClientBuilder = RestClient.builder(hosts);
        RestClientBuilderCustomizer builderCustomizer = new DefaultRestClientBuilderCustomizer(properties);
        restClientBuilder.setHttpClientConfigCallback((httpClientBuilder) -> {
            builderCustomizer.customize(httpClientBuilder);
            return httpClientBuilder;
        });
        restClientBuilder.setRequestConfigCallback((requestConfigBuilder) -> {
            builderCustomizer.customize(requestConfigBuilder);
            return requestConfigBuilder;
        });
        builderCustomizer.customize(restClientBuilder);
        return restClientBuilder;
    }

    private static HttpHost createHttpHost(String uri) {
        try {
            return createHttpHost(URI.create(uri));
        } catch (IllegalArgumentException ex) {
            return HttpHost.create(uri);
        }
    }

    private static HttpHost createHttpHost(URI uri) {
        if (!StringUtils.hasLength(uri.getUserInfo())) {
            return HttpHost.create(uri.toString());
        }
        try {
            return HttpHost.create(new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(),
                    uri.getQuery(), uri.getFragment()).toString());
        } catch (URISyntaxException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
