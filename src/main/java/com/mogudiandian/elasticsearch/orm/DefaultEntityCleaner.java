package com.mogudiandian.elasticsearch.orm;

import com.mogudiandian.elasticsearch.orm.datasource.ElasticsearchDatasource;
import com.mogudiandian.elasticsearch.orm.datasource.ElasticsearchDatasourceDelegator;
import com.mogudiandian.elasticsearch.orm.core.EntityMapping;
import com.mogudiandian.elasticsearch.orm.core.EntityMappingFinder;
import com.mogudiandian.elasticsearch.orm.core.EntityProperty;
import com.mogudiandian.elasticsearch.orm.core.annotation.LifecycleField;
import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 默认的实体数据清理器
 * 使用delete_by_query + force_merge
 * @author sunbo
 */
@Slf4j
@Component
public class DefaultEntityCleaner implements EntityCleaner {

    @Autowired
    private ElasticsearchDatasourceDelegator elasticSearchDatasourceDelegator;

    @Override
    public void clean(Class<? extends BaseEntity> entityClass) {

        EntityMapping<? extends BaseEntity> entityMapping = EntityMappingFinder.find(entityClass);

        LifecycleField lifecycleField = OrmUtils.getEntityProperties(entityClass)
                                                .stream()
                                                .filter(EntityProperty::isForLifecycle)
                                                .map(EntityProperty::getLifecycleField)
                                                .findFirst()
                                                .orElse(null);

        if (lifecycleField != null) {
            // 当前时间-生命周期 表示最大的时间 清理该时间之前的数据
            long maxTime = System.currentTimeMillis() - lifecycleField.durationUnit().toMillis(lifecycleField.value());

            // objectType in () and time < xxx
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            queryBuilder.must(QueryBuilders.termQuery(BaseEntity.CLAZZ, entityClass.getName()));
            queryBuilder.must(QueryBuilders.rangeQuery(entityMapping.lifecycleField()).lt(maxTime));

            String indexName = OrmUtils.getIndexName(entityClass);

            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(indexName);
            deleteByQueryRequest.setQuery(queryBuilder);
            deleteByQueryRequest.setBatchSize(100);
            deleteByQueryRequest.setScroll(TimeValue.timeValueMinutes(10));
            deleteByQueryRequest.setRefresh(true);

            ElasticsearchDatasource datasource = elasticSearchDatasourceDelegator.delegate();
            datasource.deleteByQueryAsync(deleteByQueryRequest, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {
                @Override
                public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                    // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.9/java-rest-high-document-delete-by-query.html

                    TimeValue timeTaken = bulkByScrollResponse.getTook();
                    log.debug("total time taken: {}", timeTaken);
                    boolean timedOut = bulkByScrollResponse.isTimedOut();
                    log.debug("Check if the request timed out: {}", timedOut);
                    long totalDocs = bulkByScrollResponse.getTotal();
                    log.debug("Get total number of docs processed: {}", totalDocs);
                    long deletedDocs = bulkByScrollResponse.getDeleted();
                    log.debug("Number of docs that were deleted: {}", deletedDocs);
                    long batches = bulkByScrollResponse.getBatches();
                    log.debug("Number of batches that were executed: {}", batches);
                    long noops = bulkByScrollResponse.getNoops();
                    log.debug("Number of skipped docs: {}", noops);
                    long versionConflicts = bulkByScrollResponse.getVersionConflicts();
                    log.debug("Number of version conflicts: {}", versionConflicts);
                    long bulkRetries = bulkByScrollResponse.getBulkRetries();
                    log.debug("Number of times request had to retry bulk index operations: {}", bulkRetries);
                    long searchRetries = bulkByScrollResponse.getSearchRetries();
                    log.debug("Number of times request had to retry search operations: {}", searchRetries);
                    TimeValue throttledMillis = bulkByScrollResponse.getStatus().getThrottled();
                    log.debug("The total time this request has throttled itself not including the current throttle time if it is currently sleeping: {}", throttledMillis);
                    TimeValue throttledUntilMillis = bulkByScrollResponse.getStatus().getThrottledUntil();
                    log.debug("Remaining delay of any current throttle sleep or 0 if not sleeping: {}", throttledUntilMillis);
                    List<ScrollableHitSource.SearchFailure> searchFailures = bulkByScrollResponse.getSearchFailures();
                    log.debug("Failures during search phase: {}", searchFailures);
                    List<BulkItemResponse.Failure> bulkFailures = bulkByScrollResponse.getBulkFailures();
                    log.debug("Failures during bulk index operation: {}", bulkFailures);

                    if (deletedDocs > 0) {
                        ForceMergeRequest forceMergeRequest = new ForceMergeRequest(indexName);
                        forceMergeRequest.flush(true);

                        datasource.indices().forcemergeAsync(forceMergeRequest, RequestOptions.DEFAULT, new ActionListener<ForceMergeResponse>() {
                            @Override
                            public void onResponse(ForceMergeResponse forceMergeResponse) {
                                // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-force-merge.html

                                int totalShards = forceMergeResponse.getTotalShards();
                                log.debug("Total number of shards hit by the force merge request: {}", totalShards);
                                int successfulShards = forceMergeResponse.getSuccessfulShards();
                                log.debug("Number of shards where the force merge has succeeded: {}", successfulShards);
                                int failedShards = forceMergeResponse.getFailedShards();
                                log.debug("Number of shards where the force merge has failed: {}", failedShards);
                                DefaultShardOperationFailedException[] failures = forceMergeResponse.getShardFailures();
                                log.debug("A list of failures if the operation failed on one or more shards: {}{}", failures, "");
                            }

                            @Override
                            public void onFailure(Exception e) {
                                log.error("force merge error, request is: {}, exception is: ", forceMergeRequest, e);
                            }
                        });
                    }
                }
                @Override
                public void onFailure(Exception e) {
                    log.error("delete by query error, request is: {}, exception is: ", deleteByQueryRequest, e);
                }
            });
        }
    }

}
