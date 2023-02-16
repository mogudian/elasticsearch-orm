package com.mogudiandian.elasticsearch.orm.core.request.api.simple;

import com.mogudiandian.elasticsearch.orm.core.request.api.ApiSearchRequest;
import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.Setter;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 简单搜索条件
 * @author sunbo
 */
public class SimpleSearchRequest extends ApiSearchRequest {

    /**
     * 更多搜索条件
     */
    @Setter
    private LinkedHashMap<String, Object> moreQueryFields;

    public SimpleSearchRequest(Class<? extends BaseEntity> entityClass) {
        super(entityClass);
    }

    public SimpleSearchRequest appendQueryField(String fieldName, Object value) {
        if (moreQueryFields == null) {
            moreQueryFields = new LinkedHashMap<>();
        }
        moreQueryFields.put(fieldName, value);
        return this;
    }

    public SimpleSearchRequest removeQueryField(String fieldName) {
        if (moreQueryFields != null) {
            moreQueryFields.remove(fieldName);
        }
        return this;
    }

    public SimpleSearchRequest clearQueryFields() {
        if (moreQueryFields != null) {
            moreQueryFields.clear();
        }
        return this;
    }

    public boolean hasMoreQueryFields() {
        return moreQueryFields != null && !moreQueryFields.isEmpty();
    }

    @Override
    public QueryBuilder toQueryBuilder() {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (hasMoreQueryFields()) {
            // TODO 根据.号分组
            for (Map.Entry<String, Object> entry : moreQueryFields.entrySet()) {
                String fieldName = entry.getKey();
                // 根据实体属性的字段名进行replace
                fieldName = OrmUtils.getEntityFieldName(entityClass, fieldName);
                Object value = entry.getValue();
                if (value != null) {
                    Class<?> cls = value.getClass();
                    if (cls.isArray()) {
                        List<Object> list = IntStream.range(0, Array.getLength(value))
                                                        .mapToObj(x -> Array.get(value, x))
                                                        .collect(Collectors.toList());
                        boolQueryBuilder.must(QueryBuilders.termsQuery(fieldName, list));
                    } else if (Collection.class.isAssignableFrom(cls)) {
                        boolQueryBuilder.must(QueryBuilders.termsQuery(fieldName, (Collection<?>) value));
                    } else{
                        boolQueryBuilder.must(QueryBuilders.termQuery(fieldName, value));
                    }
                }
            }
        }
        return boolQueryBuilder;
    }
}
