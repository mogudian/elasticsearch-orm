package com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain;

import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.hints.Hint;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents abstract query. every query
 * has indexes, types, and where clause.
 */
public abstract class Query {

    private Where where = null;

    private final List<From> from = new ArrayList<>();

    private int offset;

    private int rowCount = -1;
    
    private final List<Hint> hints = new ArrayList<>();

    public Where getWhere() {
        return this.where;
    }

    public void setWhere(Where where) {
        this.where = where;
    }

    public List<From> getFrom() {
        return from;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public List<Hint> getHints() {
        return hints;
    }

}
