package com.mogudiandian.elasticsearch.orm.schedule;

import com.mogudiandian.elasticsearch.orm.datasource.ElasticsearchDatasource;
import com.mogudiandian.elasticsearch.orm.datasource.ElasticsearchDatasourceDelegator;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 实体清理的定时任务依赖的锁默认实现
 * 使用ES实现的分布式锁
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Slf4j
@Setter
public class DefaultEntityCleanLocker implements EntityCleanLocker {

    private ElasticsearchDatasourceDelegator elasticSearchDatasourceDelegator;

    private String getLockIndexName() {
        return "es-orm-clean-locks";
    }

    /**
     * 获取或创建索引
     * @return 索引名
     */
    private String getOrCreateIndex() {
        String name = getLockIndexName();
        CreateIndexRequest request = new CreateIndexRequest(name);
        // 单点
        request.settings(Settings.builder()
                                 .put("index.number_of_shards", 1)
                                 .put("index.number_of_replicas", 0));

        ElasticsearchDatasource datasource = elasticSearchDatasourceDelegator.delegate();
        try {
            datasource.indices().create(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.warn("create index {} failure, exception is: ", name, e);
            if (e.getMessage() != null && e.getMessage().contains("already") && e.getMessage().contains("exists")) {
                log.warn("index {} is already exists", name);
            }
        }
        return name;
    }

    /**
     * 生成ID
     * @return yyyyMMdd
     */
    private String generateId() {
        return new SimpleDateFormat("yyyyMMdd").format(new Date());
    }

    /**
     * 添加文档
     * @return 是否成功
     */
    private boolean addDocument() {
        String name = getLockIndexName();
        String id = generateId();
        Map<String, Object> doc = new HashMap<>(2);
        doc.put("uuid", UUID.randomUUID().toString().replace("-", ""));
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest(name).create(true).id(id).source(doc));

        ElasticsearchDatasource datasource = elasticSearchDatasourceDelegator.delegate();
        try {
            BulkResponse response = datasource.bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures()) {
                for (BulkItemResponse bulkItemResponse : response) {
                    if (bulkItemResponse.getFailure() != null && bulkItemResponse.getFailure().getStatus() == RestStatus.CONFLICT) {
                        return false;
                    }
                    throw new RuntimeException(bulkItemResponse.getFailureMessage());
                }
            }
        } catch (Exception e) {
            log.warn("add document into index {}/{} {} failure, exception is: ", name, id, doc, e);
            throw new RuntimeException(e);
        }
        log.debug("lock {} with {} success", id, doc);
        return true;
    }

    /**
     * 删除文档
     * @return 是否成功
     */
    private boolean deleteDocument() {
        String name = getLockIndexName();
        String id = generateId();
        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest(name, id));

        ElasticsearchDatasource datasource = elasticSearchDatasourceDelegator.delegate();
        try {
            BulkResponse response = datasource.bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures()) {
                throw new RuntimeException(response.buildFailureMessage());
            }
        } catch (Exception e) {
            log.warn("delete document {} from index {} failure, exception is: ", id, name, e);
            throw new RuntimeException(e);
        }
        log.debug("unlock {} success", id);
        return true;
    }

    @Override
    public boolean tryLock() {
        try {
            getOrCreateIndex();
            addDocument();
            return true;
        } catch (Exception e) {
            log.error("clean entity failure when try lock, exception is: ", e);
            return false;
        }
    }

    @Override
    public void unlock() {
        deleteDocument();
    }
}
