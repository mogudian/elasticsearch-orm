package com.mogudiandian.elasticsearch.orm.core.request.api;

import com.mogudiandian.elasticsearch.orm.core.EntityMapping;
import com.mogudiandian.elasticsearch.orm.core.request.SearchRequest;
import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * 搜索条件
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public abstract class ApiSearchRequest extends SearchRequest {

    /**
     * 关键词
     */
    @Getter
    @Setter
    protected String keyword;

    /**
     * 生命周期范围
     */
    @Getter
    @Setter
    protected LifecycleRange lifecycleRange;

    /**
     * 排序
     */
    @Getter
    private List<SortConfig> sorts;

    /**
     * 分页
     */
    @Getter
    @Setter
    private PaginationConfig pagination;

    /**
     * 高亮
     */
    @Setter
    private HighlightConfig highlight;

    public ApiSearchRequest(Class<? extends BaseEntity> entityClass) {
        super(entityClass);
    }

    @Override
    public HighlightConfig getHighlight() {
        if (highlight != null && StringUtils.isNotEmpty(keyword)) {
            highlight.setKeyword(keyword);
        }
        return highlight;
    }

    /**
     * 生成搜索条件
     * @param entityMapping 实体映射
     * @param <E> 实体类型
     * @return 搜索条件
     */
    @Override
    public final <E extends BaseEntity> SearchSourceBuilder toSearchSourceBuilder(EntityMapping<E> entityMapping) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        // 根据类型查询
        queryBuilder.must(QueryBuilders.termQuery(BaseEntity.CLAZZ, this.getEntityClass().getName()));

        sourceBuilder.query(queryBuilder);

        if (StringUtils.isNotEmpty(this.getKeyword())) {
            List<String> fields = entityMapping.searchFields();
            if (!fields.isEmpty()) {
                // 根据字段进行multi_match搜索
                queryBuilder.must(QueryBuilders.multiMatchQuery(this.getKeyword(), fields.toArray(new String[0])).type(MultiMatchQueryBuilder.Type.PHRASE));

                if (this.getHighlight() != null) {
                    HighlightBuilder highlightBuilder = new HighlightBuilder();
                    fields.forEach(highlightBuilder::field);
                    highlightBuilder.requireFieldMatch(false);
                    highlightBuilder.fragmentSize(100);
                    sourceBuilder.highlighter(highlightBuilder);
                }
            }
        }

        // 根据生命周期范围 拼成millis
        if (this.getLifecycleRange() != null) {
            String field = entityMapping.lifecycleField();
            if (field != null) {
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field);
                // 是否需要拼range
                boolean range = false;
                if (this.getLifecycleRange().getStart() != null) {
                    rangeQueryBuilder.gte(this.getLifecycleRange().getStart().getTime());
                    range = true;
                }
                if (this.getLifecycleRange().getEnd() != null) {
                    rangeQueryBuilder.lt(this.getLifecycleRange().getEnd().getTime());
                    range = true;
                }
                if (range) {
                    queryBuilder.must(rangeQueryBuilder);
                }
            }
        }

        queryBuilder.must(this.toQueryBuilder());

        if (this.sorts != null && !this.sorts.isEmpty()) {
            for (SortConfig sort : this.sorts) {
                // 根据实体属性的字段名进行replace
                String fieldName = OrmUtils.getEntityFieldName(entityClass, sort.getField());
                sourceBuilder.sort(fieldName, sort.isDescending() ? SortOrder.DESC : SortOrder.ASC);
            }
        }

        if (this.getPagination() != null) {
            sourceBuilder.from(this.getPagination().getOffset())
                         .size(this.getPagination().getPageSize());
        }

        return sourceBuilder;
    }

    /**
     * 添加排序条件
     * @param sorts 排序条件 可变长
     */
    public void addSorts(SortConfig... sorts) {
        if (this.sorts == null) {
            this.sorts = new LinkedList<>();
        }
        Collections.addAll(this.sorts, sorts);
    }

    /**
     * 将搜索条件拼接为QueryBuilder
     * @return ES的Query
     */
    public abstract QueryBuilder toQueryBuilder();

}
