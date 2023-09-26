package com.mogudiandian.elasticsearch.orm.datasource;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.boot.autoconfigure.elasticsearch.RestClientBuilderCustomizer;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.time.Duration;

/**
 * 默认的ES-rest-client定制器
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public class DefaultRestClientBuilderCustomizer implements RestClientBuilderCustomizer {

    private static final PropertyMapper map = PropertyMapper.get();

    private final ElasticsearchProperties properties;

    public DefaultRestClientBuilderCustomizer(ElasticsearchProperties properties) {
        this.properties = properties;
    }

    @Override
    public void customize(RestClientBuilder builder) {
    }

    @Override
    public void customize(HttpAsyncClientBuilder builder) {
        builder.setDefaultCredentialsProvider(new PropertiesCredentialsProvider(this.properties));
    }

    @Override
    public void customize(RequestConfig.Builder builder) {
        map.from(this.properties::getConnectionTimeout).whenNonNull().asInt(Duration::toMillis)
           .to(builder::setConnectTimeout);
        map.from(this.properties::getReadTimeout).whenNonNull().asInt(Duration::toMillis)
           .to(builder::setSocketTimeout);
    }

    public static class PropertiesCredentialsProvider extends BasicCredentialsProvider {

        public PropertiesCredentialsProvider(ElasticsearchProperties properties) {
            if (StringUtils.hasText(properties.getUsername())) {
                Credentials credentials = new UsernamePasswordCredentials(properties.getUsername(),
                        properties.getPassword());
                setCredentials(AuthScope.ANY, credentials);
            }
            properties.getUris().stream().map(this::toUri).filter(this::hasUserInfo)
                      .forEach(this::addUserInfoCredentials);
        }

        private URI toUri(String uri) {
            try {
                return URI.create(uri);
            } catch (IllegalArgumentException ex) {
                return null;
            }
        }

        private boolean hasUserInfo(URI uri) {
            return uri != null && StringUtils.hasLength(uri.getUserInfo());
        }

        private void addUserInfoCredentials(URI uri) {
            AuthScope authScope = new AuthScope(uri.getHost(), uri.getPort());
            Credentials credentials = createUserInfoCredentials(uri.getUserInfo());
            setCredentials(authScope, credentials);
        }

        private Credentials createUserInfoCredentials(String userInfo) {
            int delimiter = userInfo.indexOf(":");
            if (delimiter == -1) {
                return new UsernamePasswordCredentials(userInfo, null);
            }
            String username = userInfo.substring(0, delimiter);
            String password = userInfo.substring(delimiter + 1);
            return new UsernamePasswordCredentials(username, password);
        }

    }
}
