package com.mogudiandian.elasticsearch.orm.datasource;

/**
 * 单ES数据源委托者
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public class SingleElasticsearchDatasourceDelegator implements ElasticsearchDatasourceDelegator {

    /**
     * 数据源
     */
    private final ElasticsearchDatasource elasticSearchDatasource;

    public SingleElasticsearchDatasourceDelegator(ElasticsearchDatasource elasticSearchDatasource) {
        this.elasticSearchDatasource = elasticSearchDatasource;
    }

    @Override
    public ElasticsearchDatasource delegate() {
        return elasticSearchDatasource;
    }
}
