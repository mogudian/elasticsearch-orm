package com.mogudiandian.elasticsearch.orm;

import com.mogudiandian.elasticsearch.orm.core.request.SearchRequest;
import com.mogudiandian.elasticsearch.orm.core.response.SearchResult;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;

import java.util.List;
import java.util.Map;

/**
 * 任务搜索client
 * @author sunbo
 */
public interface ElasticsearchOrmClient {

    /**
     * 添加实体到ES
     * @param entity 实体
     * @param <E> ES的实体类型
     * @return document
     */
    <E extends BaseEntity> Map<String, Object> add(E entity);

    /**
     * 批量添加实体到ES
     * @param entities 实体集合
     * @param <E> 实体类型
     * @return document集合
     */
    <E extends BaseEntity> List<Map<String, Object>> addAll(List<E> entities);

    /**
     * 搜索
     * @param searchRequest 搜索条件
     * @param <E> 实体类型
     * @return 搜索结果
     */
    <E extends BaseEntity> SearchResult<E> search(SearchRequest searchRequest);
}
