package com.mogudiandian.elasticsearch.orm.core.request.api;

import lombok.Setter;

/**
 * 分页条件
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Setter
public class PaginationConfig {

    private static final Integer DEFAULT_PAGE_NO = 1, DEFAULT_PAGE_SIZE = 10;

    /**
     * 页码
     */
    private Integer pageNo;

    /**
     * 每页条数
     */
    private Integer pageSize;

    public PaginationConfig(Integer pageSize) {
        this(1, pageSize);
    }

    private PaginationConfig(Integer pageNo, Integer pageSize) {
        this.pageNo = pageNo;
        this.pageSize = pageSize;
    }

    public static PaginationConfig buildByPage(Integer pageNo, Integer pageSize) {
        return new PaginationConfig(pageNo, pageSize);
    }

    public static PaginationConfig buildByOffset(Integer offset, Integer pageSize) {
        return new PaginationConfig(offset / pageSize + 1, pageSize);
    }

    public Integer getPageNo() {
        return pageNo != null && pageNo > 0 ? pageNo : DEFAULT_PAGE_NO;
    }

    public Integer getPageSize() {
        return pageSize != null && pageSize > 0 ? pageSize : DEFAULT_PAGE_SIZE;
    }

    public Integer getOffset() {
        return (getPageNo() - 1) * getPageSize();
    }
}
