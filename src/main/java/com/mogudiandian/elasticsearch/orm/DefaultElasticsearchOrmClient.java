package com.mogudiandian.elasticsearch.orm;

import com.mogudiandian.elasticsearch.orm.datasource.ElasticsearchDatasource;
import com.mogudiandian.elasticsearch.orm.datasource.ElasticsearchDatasourceDelegator;
import com.mogudiandian.elasticsearch.orm.configuration.ElasticsearchOrmProperties;
import com.mogudiandian.elasticsearch.orm.core.EntityMapping;
import com.mogudiandian.elasticsearch.orm.core.EntityMappingFinder;
import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;
import com.mogudiandian.elasticsearch.orm.core.request.SearchRequest;
import com.mogudiandian.elasticsearch.orm.core.response.SearchResult;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 任务搜索client默认实现
 * @author sunbo
 */
@Slf4j
@Getter
@Component
public class DefaultElasticsearchOrmClient implements ElasticsearchOrmClient {

    @Autowired
    private ElasticsearchOrmProperties properties;

    @Autowired
    private ElasticsearchDatasourceDelegator elasticSearchDatasourceDelegator;

    @Override
    public <E extends BaseEntity> Map<String, Object> add(E entity) {
        Objects.requireNonNull(entity, "实体不能为空");

        // 获取内容类型和映射
        EntityMapping<E> entityMapping = EntityMappingFinder.find((Class<E>) entity.getClass());

        // 将实体转换为map
        Map<String, Object> map = entityMapping.convert(entity);

        BulkRequest request = new BulkRequest();
        request.timeout(TimeValue.timeValueMillis(properties.getUpdateTimeout()));

        String indexName = OrmUtils.getIndexName(entity.getClass());
        request.add(new IndexRequest(indexName).id(entity.entityId()).source(map));

        RestStatus restStatus = addDocument(request);
        log.debug("add document to elastic search index={} finished, status={}, entity[{}] is: {}", indexName, restStatus, entity.entityId(), entity);

        return map;
    }

    @Override
    public <E extends BaseEntity> List<Map<String, Object>> addAll(List<E> entities) {
        Objects.requireNonNull(entities, "实体集合不能为空");

        // 根据索引分组
        Map<? extends Class<? extends BaseEntity>, List<E>> classGroup = entities.stream()
                                                                                 .collect(Collectors.groupingBy(x -> x.getClass()));

        BulkRequest request = new BulkRequest();
        request.timeout(TimeValue.timeValueMillis(properties.getUpdateTimeout()));

        List<Map<String, Object>> maps = new ArrayList<>(entities.size());

        // 根据不同的组 添加到不同的索引
        for (Map.Entry<? extends Class<? extends BaseEntity>, List<E>> entry : classGroup.entrySet()) {
            Class<? extends BaseEntity> entityClass = entry.getKey();
            String indexName = OrmUtils.getIndexName(entityClass);
            EntityMapping<E> entityMapping = (EntityMapping<E>) EntityMappingFinder.find(entityClass);
            for (E entity : entry.getValue()) {
                Map<String, Object> map = entityMapping.convert(entity);
                request.add(new IndexRequest(indexName).id(entity.entityId()).source(map));
                maps.add(map);
            }
        }

        RestStatus restStatus = addDocument(request);
        log.debug("add documents to elastic search finished, status={}, entities are: {}", restStatus, entities);

        return maps;
    }

    private RestStatus addDocument(BulkRequest request) {
        try {
            ElasticsearchDatasource datasource = elasticSearchDatasourceDelegator.delegate();
            BulkResponse response = datasource.bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures()) {
                throw new RuntimeException("add document to elastic search failure, message is: " + response.buildFailureMessage());
            }
            return response.status();
        } catch (Exception e) {
            throw new RuntimeException("add document to elastic search error", e);
        }
    }

    @Override
    public <E extends BaseEntity> SearchResult<E> search(SearchRequest searchRequest) {

        String indexName = OrmUtils.getIndexName(searchRequest.getEntityClass());

        // 填充request的上下文
        searchRequest.setIndexName(indexName);
        searchRequest.setDefaultDateFormat(properties.getDefaultDateFormat());

        EntityMapping<E> entityMapping = (EntityMapping<E>) EntityMappingFinder.find(searchRequest.getEntityClass());

        SearchSourceBuilder sourceBuilder = searchRequest.toSearchSourceBuilder(entityMapping);

        sourceBuilder.timeout(TimeValue.timeValueMillis(properties.getQueryTimeout()));
        sourceBuilder.trackTotalHits(true);

        log.debug("query condition is: " + sourceBuilder.toString());

        org.elasticsearch.action.search.SearchRequest request = new org.elasticsearch.action.search.SearchRequest(new String[]{indexName}, sourceBuilder);

        try {
            ElasticsearchDatasource datasource = elasticSearchDatasourceDelegator.delegate();
            SearchResponse searchResponse = datasource.search(request, RequestOptions.DEFAULT);

            SearchHits searchHits = searchResponse.getHits();
            long total = searchHits.getTotalHits().value;

            log.debug("hit record count is: "  + total);
            if (searchRequest.getPagination() != null) {
                log.debug("当前页数: " + searchRequest.getPagination().getPageNo());
                log.debug("总页码数: " + (total + searchRequest.getPagination().getPageSize() - 1) / searchRequest.getPagination().getPageSize());
            }

            SearchResult<E> searchResult = new SearchResult<>();
            if (searchRequest.getPagination() != null) {
                searchResult.setCurrentPage(searchRequest.getPagination().getPageNo());
                searchResult.setPageSize(searchRequest.getPagination().getPageSize());
                searchResult.setTotalCount(total);
            }

            SearchHit[] hits = searchHits.getHits();
            for (SearchHit hit : hits) {
                log.debug("文档ID: " + hit.getId());
                log.debug("文档得分: " + hit.getScore());
                log.debug("文档: " + hit.getSourceAsMap());

                Map<String, Object> documentMap = hit.getSourceAsMap();

                // 如果需要高亮 使用高亮后的替换掉原始的内容
                if (searchRequest.getHighlight() != null) {
                    Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                    for (Map.Entry<String, HighlightField> entry : highlightFields.entrySet()) {
                        HighlightField highlightField = entry.getValue();
                        if (highlightField != null && highlightField.fragments() != null) {
                            String fragments = Arrays.stream(highlightField.fragments())
                                                     .map(Text::string)
                                                     .collect(Collectors.joining());
                            documentMap.put(entry.getKey(), fragments);
                        }
                    }
                }

                E entity = entityMapping.convert(documentMap);

                searchResult.addRecord(entity, hit.getScore());
            }

            if (searchRequest.isDebug()) {
                searchResult.setQueryString(sourceBuilder.toString());
            }

            return searchResult;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
