package com.mogudiandian.elasticsearch.orm.datasource;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * ES多数据源委托者
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public class MultiElasticsearchDatasourceDelegator<K extends Serializable> implements ElasticsearchDatasourceDelegator {

    /**
     * 切换数据源的标记
     */
    private final ThreadLocal<K> datasourceKey;

    /**
     * 默认数据源
     */
    private final ElasticsearchDatasource defaultDatasource;

    /**
     * 数据源标识 -> 数据源
     */
    protected Map<K, ElasticsearchDatasource> datasourceMap = new HashMap<>();

    public MultiElasticsearchDatasourceDelegator(ThreadLocal<K> datasourceKey) {
        this.datasourceKey = datasourceKey;
        this.defaultDatasource = null;
    }

    public MultiElasticsearchDatasourceDelegator(ThreadLocal<K> datasourceKey, ElasticsearchDatasource defaultDatasource) {
        this.datasourceKey = datasourceKey;
        this.defaultDatasource = defaultDatasource;
    }

    /**
     * 添加数据源
     * @param key 数据源标识
     * @param datasource 数据源
     */
    public void addDatasource(K key, ElasticsearchDatasource datasource) {
        datasourceMap.put(key, datasource);
    }

    @Override
    public ElasticsearchDatasource delegate() {
        ElasticsearchDatasource datasource = datasourceMap.getOrDefault(datasourceKey.get(), defaultDatasource);
        if (datasource == null) {
            throw new RuntimeException("No such datasource: " + datasourceKey.get());
        }
        return datasource;
    }

}
