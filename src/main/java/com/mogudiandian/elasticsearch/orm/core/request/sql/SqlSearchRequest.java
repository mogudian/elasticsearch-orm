package com.mogudiandian.elasticsearch.orm.core.request.sql;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;
import com.mogudiandian.elasticsearch.orm.core.EntityMapping;
import com.mogudiandian.elasticsearch.orm.core.request.SearchRequest;
import com.mogudiandian.elasticsearch.orm.core.request.api.HighlightConfig;
import com.mogudiandian.elasticsearch.orm.core.request.api.PaginationConfig;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.QueryMaker;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Select;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.ElasticSqlExprParser;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.SqlParser;
import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Order;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 * 搜索条件
 *
 * @author sunbo
 */
public class SqlSearchRequest extends SearchRequest {

    /**
     * YQL
     */
    private SQL sql;

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

    public SqlSearchRequest(Class<? extends BaseEntity> entityClass, SQL sql) {
        super(entityClass);
        this.sql = sql;
    }

    public SqlSearchRequest(Class<? extends BaseEntity> entityClass, String sql) {
        super(entityClass);
        this.sql = new SQL(sql);
    }

    public SqlSearchRequest(Class<? extends BaseEntity> entityClass, String sql, Object... parameters) {
        super(entityClass);
        this.sql = new SQL(sql, parameters);
    }

    public void setParameters(Object... parameters) {
        if (this.sql != null) {
            this.sql.setParameters(parameters);
        }
    }

    /**
     * 复写父类的方法 为了能够将format设置到SQL中
     *
     * @param format 时间格式
     */
    @Override
    public void setDefaultDateFormat(String format) {
        super.setDefaultDateFormat(format);
        if (this.sql != null) {
            this.sql.setDefaultDateFormat(defaultDateFormat);
        }
    }

    @Override
    public HighlightConfig getHighlight() {
        return highlight;
    }

    /**
     * 生成搜索条件
     *
     * @param entityMapping 实体映射
     * @param <E>           实体类型
     * @return 搜索条件
     */
    @Override
    public final <E extends BaseEntity> SearchSourceBuilder toSearchSourceBuilder(EntityMapping<E> entityMapping) {
        if (sql == null) {
            throw new NullPointerException("yql must be set");
        }

        String sql = "select * from " + getIndexName() + " " + this.sql.toString();

        // 将SQL解析成AST，即SQLQueryExpr sqlExpr就是AST了，下面的代码就开始访问AST、从中获取token
        SQLExprParser parser = new ElasticSqlExprParser(sql);
        // 解析SQL，得到的AST
        SQLQueryExpr sqlExpr = (SQLQueryExpr) parser.expr();

        // 调用parser.expr()方法解析完sql语句后，发现最后一个token不是End Of File的话，即该sql语句貌似是残缺的，可能是用户输入了一个未结束的sql
        if (parser.getLexer().token() != Token.EOF) {
            throw new ParserException("illegal sql expr : " + sql);
        }
        Select select = new SqlParser().parseSelect(sqlExpr);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        // 根据类型查询
        queryBuilder.must(QueryBuilders.termQuery(BaseEntity.CLAZZ, this.getEntityClass().getName()));

        // where
        queryBuilder.must(new QueryMaker(entityClass).explain(select.getWhere(), select.isFilter));

        sourceBuilder.query(queryBuilder);

        if (select.getOrderBys() != null && !select.getOrderBys().isEmpty()) {
            for (Order orderBy : select.getOrderBys()) {
                if (orderBy.getNestedPath() != null) {
                    sourceBuilder.sort(SortBuilders.fieldSort(orderBy.getName()).order(SortOrder.valueOf(orderBy.getType())).setNestedSort(new NestedSortBuilder(orderBy.getNestedPath())));
                } else if (orderBy.getName().contains("script(")) {
                    // 兼容order by case when 这种情况
                    String scriptStr = orderBy.getName().substring("script(".length(), orderBy.getName().length() - 1);
                    Script script = new Script(scriptStr);
                    ScriptSortBuilder scriptSortBuilder = SortBuilders.scriptSort(script, orderBy.getScriptSortType())
                                                                      .order(SortOrder.valueOf(orderBy.getType()));
                    sourceBuilder.sort(scriptSortBuilder);
                } else {
                    // 根据实体属性的字段名进行replace
                    String fieldName = OrmUtils.getEntityFieldName(entityClass, orderBy.getName());
                    sourceBuilder.sort(fieldName, SortOrder.valueOf(orderBy.getType()));
                }
            }
        }

        if (this.getPagination() == null && (select.getOffset() >= 0 && select.getRowCount() > 0)) {
            this.pagination = PaginationConfig.buildByOffset(select.getOffset(), select.getRowCount());
        }

        if (this.getPagination() != null) {
            sourceBuilder.from(this.getPagination().getOffset())
                         .size(this.getPagination().getPageSize());
        }

        return sourceBuilder;
    }

}
