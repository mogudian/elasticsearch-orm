package com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain;

import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.SubQueryExpression;
import org.elasticsearch.search.sort.ScriptSortBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 将sql语句转换为select 对象
 *
 * @author ansj
 */
public class Select extends Query {

    // 增加对移动平均值和滚动标准差的支持
    private final List<String> aggrFunctions = Arrays.asList("SUM", "MAX", "MIN", "AVG", "TOPHITS", "COUNT", "STATS", "EXTENDED_STATS", "PERCENTILES", "SCRIPTED_METRIC", "PERCENTILE_RANKS", "MOVINGAVG", "ROLLINGSTD");

    private List<Field> fields = new ArrayList<>();

    private List<List<Field>> groupBys = new ArrayList<>();

    private List<Order> orderBys = new ArrayList<>();

    private boolean containsSubQueries;

    private List<SubQueryExpression> subQueries;

    public boolean isFilter = false;

    private boolean selectAll = false;

    private String having;

    public List<Field> getFields() {
        return fields;
    }

    public void addGroupBy(Field field) {
        List<Field> wrapper = new ArrayList<>();
        wrapper.add(field);
        addGroupBy(wrapper);
    }

    public String getHaving() {
        return having;
    }

    public void setHaving(String having) {
        this.having = having;
    }

    public void addGroupBy(List<Field> fields) {
        this.groupBys.add(fields);
    }

    public List<List<Field>> getGroupBys() {
        return groupBys;
    }

    public List<Order> getOrderBys() {
        return orderBys;
    }

    public void addOrderBy(String nestedPath, String name, String type, ScriptSortBuilder.ScriptSortType scriptSortType) {
        // 可以直接在order by子句中写_score，根据该字段排序 select * from tbl order by _score asc
        if ("_score".equals(name)) {
            isFilter = false;
        }
        Order order = new Order(nestedPath, name, type);

        order.setScriptSortType(scriptSortType);
        this.orderBys.add(order);
    }


    public void addField(Field field) {
        if (field == null) {
            return;
        }
        if (field.getName().equals("*")) {
            this.selectAll = true;
        }
        fields.add(field);
    }

    public void fillSubQueries() {
        subQueries = new ArrayList<>();
        Where where = this.getWhere();
        fillSubQueriesFromWhereRecursive(where);
    }

    private void fillSubQueriesFromWhereRecursive(Where where) {
        if (where == null) return;
        if (where instanceof Condition) {
            Condition condition = (Condition) where;
            if (condition.getValue() instanceof SubQueryExpression) {
                this.subQueries.add((SubQueryExpression) condition.getValue());
                this.containsSubQueries = true;
            }
            if (condition.getValue() instanceof Object[]) {

                for (Object o : (Object[]) condition.getValue()) {
                    if (o instanceof SubQueryExpression) {
                        this.subQueries.add((SubQueryExpression) o);
                        this.containsSubQueries = true;
                    }
                }
            }
        } else {
            for (Where innerWhere : where.getWheres())
                fillSubQueriesFromWhereRecursive(innerWhere);
        }
    }

    public boolean containsSubQueries() {
        return containsSubQueries;
    }

    public List<SubQueryExpression> getSubQueries() {
        return subQueries;
    }

    public boolean isOrderdSelect() {
        return this.getOrderBys() != null && this.getOrderBys().size() > 0;
    }

    public boolean isSelectAll() {
        return selectAll;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
}

