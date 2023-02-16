package com.mogudiandian.elasticsearch.orm.datasource;

/**
 * ES数据源委托者
 * @author sunbo
 */
public interface ElasticsearchDatasourceDelegator {

    /**
     * 委托给获取ES数据源
     * @return ES数据源
     */
    ElasticsearchDatasource delegate();

}
