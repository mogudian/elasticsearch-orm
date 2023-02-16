package com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Select;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.hints.Hint;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Field;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.From;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Query;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.hints.HintFactory;
import org.elasticsearch.search.sort.ScriptSortBuilder;

import java.util.ArrayList;
import java.util.List;


/**
 * es sql support
 *
 * @author ansj
 */
public class SqlParser {

    public Select parseSelect(SQLQueryExpr mySqlExpr) {
        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) mySqlExpr.getSubQuery().getQuery();
        return parseSelect(query);
    }

    /**
     * 在访问AST里面的子句和token
     */
    public Select parseSelect(MySqlSelectQueryBlock query) {

        Select select = new Select();
        /*
         * SqlParser类没有成员变量，里面全是方法，所以将this传到WhereParser对象时是无状态的，
         * 即SqlParser对象并没有给WhereParser传递任何属性，也不存在WhereParser修改SqlParser的成员变量值这一说
         * WhereParser只是单纯想调用SqlParser的方法而已
         */
        WhereParser whereParser = new WhereParser(this, query);

        /*
         * 例如sql：select   a,sum(b),case when c='a' then 1 else 2 end as my_c from tbl，
         * 那findSelect()就是解析这一部分了：a,sum(b),case when c='a' then 1 else 2 end as my_c
         */
        findSelect(query, select, query.getFrom().getAlias());

        select.getFrom().addAll(findFrom(query.getFrom()));
        select.setWhere(whereParser.findWhere());

        select.fillSubQueries();

        // 解析sql语句中的hint(注释)：select /*! USE_SCROLL(10,120000) */ * FROM spark_es_table
        // /* 和 */之间是sql的注释内容，这是sql本身的语法，然后sql解析器会将注释块之间的内容“! USE_SCROLL(10,120000) ”抽取出来
        // ! USE_SCROLL是es-sql自己定义的一套规则，
        // 在不增加mysql原有语法的情况下，利用注释来灵活地扩展es-sql的功能，这样就能使用druid的mysql语法解析器了，无需自己实现
        // 注意：!叹号和USE_SCROLL之间要空且只能空一格
        select.getHints().addAll(parseHints(query.getHints()));

        findLimit(query.getLimit(), select);

        findOrderBy(query, select);

        findGroupBy(query, select);

        return select;
    }

    private void findSelect(MySqlSelectQueryBlock query, Select select, String tableAlias) {
        List<SQLSelectItem> selectList = query.getSelectList();
        for (SQLSelectItem sqlSelectItem : selectList) {
            Field field = FieldMaker.makeField(sqlSelectItem.getExpr(), sqlSelectItem.getAlias(), tableAlias);
            select.addField(field);
        }
    }

    private void findGroupBy(MySqlSelectQueryBlock query, Select select) {
        SQLSelectGroupByClause groupBy = query.getGroupBy();

        // group by 增加having语法
        if (null != query.getGroupBy() && null != query.getGroupBy().getHaving()) {
            select.setHaving(query.getGroupBy().getHaving().toString());
        }

        SQLTableSource sqlTableSource = query.getFrom();
        if (groupBy == null) {
            return;
        }
        List<SQLExpr> items = groupBy.getItems();

        List<SQLExpr> standardGroupBys = new ArrayList<>();
        for (SQLExpr sqlExpr : items) {
            // TODO mysql expr patch
            if (sqlExpr instanceof MySqlOrderingExpr) {
                MySqlOrderingExpr sqlSelectGroupByExpr = (MySqlOrderingExpr) sqlExpr;
                sqlExpr = sqlSelectGroupByExpr.getExpr();
            }
            if ((!(sqlExpr instanceof SQLIdentifierExpr || sqlExpr instanceof SQLMethodInvokeExpr)) && !standardGroupBys.isEmpty()) {
                // flush the standard group bys
                // 先将standardGroupBys里面的字段传到select对象的groupBys字段中，然后给standardGroupBys分配一个没有元素的新的list
                select.addGroupBy(convertExprsToFields(standardGroupBys, sqlTableSource));
                standardGroupBys = new ArrayList<>();
            }

            if (sqlExpr instanceof SQLParensIdentifierExpr) {
                // single item with parens (should get its own aggregation)
                select.addGroupBy(FieldMaker.makeField(((SQLParensIdentifierExpr) sqlExpr).getExpr(), null, sqlTableSource.getAlias()));
            } else if (sqlExpr instanceof SQLListExpr) {
                // multiple items in their own list
                SQLListExpr listExpr = (SQLListExpr) sqlExpr;
                select.addGroupBy(convertExprsToFields(listExpr.getItems(), sqlTableSource));
            } else {
                // everything else gets added to the running list of standard group bys
                standardGroupBys.add(sqlExpr);
            }
        }
        if (!standardGroupBys.isEmpty()) {
            select.addGroupBy(convertExprsToFields(standardGroupBys, sqlTableSource));
        }
    }

    private List<Field> convertExprsToFields(List<? extends SQLExpr> exprs, SQLTableSource sqlTableSource) {
        List<Field> fields = new ArrayList<>(exprs.size());
        for (SQLExpr expr : exprs) {
            // here we suppose groupby field will not have alias,so set null in second parameter
            // case when 有别名过不了语法解析，没有别名执行下面语句会报空指针
            fields.add(FieldMaker.makeField(expr, null, sqlTableSource.getAlias()));
        }
        return fields;
    }

    private void findOrderBy(MySqlSelectQueryBlock query, Select select) {
        SQLOrderBy orderBy = query.getOrderBy();

        if (orderBy == null) {
            return;
        }
        List<SQLSelectOrderByItem> items = orderBy.getItems();

        addOrderByToSelect(select, items, null);

    }

    private void addOrderByToSelect(Select select, List<SQLSelectOrderByItem> items, String alias) {
        for (SQLSelectOrderByItem sqlSelectOrderByItem : items) {
            SQLExpr expr = sqlSelectOrderByItem.getExpr();
            Field field = FieldMaker.makeField(expr, null, null);
            String orderByName = field.toString();

            if (sqlSelectOrderByItem.getType() == null) {
                // 默认是升序排序
                sqlSelectOrderByItem.setType(SQLOrderingSpecification.ASC);
            }
            String type = sqlSelectOrderByItem.getType().toString();

            orderByName = orderByName.replace("`", "");
            if (alias != null) orderByName = orderByName.replaceFirst(alias + "\\.", "");

            ScriptSortBuilder.ScriptSortType scriptSortType = judgeIsStringSort(expr);
            select.addOrderBy(field.getNestedPath(), orderByName, type, scriptSortType);
        }
    }

    private ScriptSortBuilder.ScriptSortType judgeIsStringSort(SQLExpr expr) {
        if (expr instanceof SQLCaseExpr) {
            List<SQLCaseExpr.Item> itemList = ((SQLCaseExpr) expr).getItems();
            for (SQLCaseExpr.Item item : itemList) {
                if (item.getValueExpr() instanceof SQLCharExpr) {
                    return ScriptSortBuilder.ScriptSortType.STRING;
                }
            }
        }
        return ScriptSortBuilder.ScriptSortType.NUMBER;
    }

    private void findLimit(SQLLimit limit, Query query) {
        if (limit == null) {
            return;
        }

        query.setRowCount(Integer.parseInt(limit.getRowCount().toString()));

        if (limit.getOffset() != null) {
            query.setOffset(Integer.parseInt(limit.getOffset().toString()));
        }
    }

    /**
     * Parse the from clause
     * 只解析了一般查询和join查询，没有解析子查询
     *
     * @param from the from clause.
     * @return list of From objects represents all the sources.
     */
    private List<From> findFrom(SQLTableSource from) {
        boolean isSqlExprTable = from.getClass().isAssignableFrom(SQLExprTableSource.class);

        if (isSqlExprTable) {
            SQLExprTableSource fromExpr = (SQLExprTableSource) from;
            String[] split = fromExpr.getExpr().toString().split(",");

            ArrayList<From> fromList = new ArrayList<>();
            for (String source : split) {
                fromList.add(new From(source.trim(), fromExpr.getAlias()));
            }
            return fromList;
        }

        SQLJoinTableSource joinTableSource = ((SQLJoinTableSource) from);
        List<From> fromList = new ArrayList<>();
        fromList.addAll(findFrom(joinTableSource.getLeft()));
        fromList.addAll(findFrom(joinTableSource.getRight()));
        return fromList;
    }

    private List<Hint> parseHints(List<SQLCommentHint> sqlHints) {
        List<Hint> hints = new ArrayList<>();
        for (SQLCommentHint sqlHint : sqlHints) {
            Hint hint = HintFactory.getHintFromString(sqlHint.getText());
            if (hint != null) hints.add(hint);
        }
        return hints;
    }

}
