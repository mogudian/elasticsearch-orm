package com.mogudiandian.elasticsearch.orm.core.response;

import com.alibaba.fastjson.JSON;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;

/**
 * ES搜索结果
 * @param <E> 实体类型
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Getter
@Setter
public class SearchResult<E extends BaseEntity> {

    /**
     * 查询语句
     */
    private String queryString;

    /**
     * 当前页码
     */
    private Integer currentPage;

    /**
     * 每页条数
     */
    private Integer pageSize;

    /**
     * 总记录数
     */
    private Long totalCount;

    /**
     * 实体和分数
     */
    private List<MatchRecord<E>> list;

    public SearchResult() {
        list = new LinkedList<>();
    }

    public SearchResult<E> addRecord(E entity, Float score) {
        list.add(new MatchRecord<>(entity, score));
        return this;
    }

    public Object getQueryString() {
        try {
            return JSON.parseObject(queryString);
        } catch (Exception e) {
            return queryString;
        }
    }

    public SearchResult<E> setQueryString(String queryString) {
        this.queryString = queryString;
        return this;
    }

    /**
     * 总页数
     */
    public Long getTotalPages() {
        if (totalCount == null || pageSize == null) {
            return null;
        }
        return (totalCount + pageSize - 1) / pageSize;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
