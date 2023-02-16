package com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.ParseUtils;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Condition;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.ESOperator;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Where;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by allwefantasy on 9/3/16.
 */
public class CaseWhenParser {

    private SQLCaseExpr caseExpr;

    public CaseWhenParser(SQLCaseExpr caseExpr) {
        this.caseExpr = caseExpr;

    }

    public String parse() {
        List<String> result = new ArrayList<>();

        for (SQLCaseExpr.Item item : caseExpr.getItems()) {
            SQLExpr conditionExpr = item.getConditionExpr();

            WhereParser parser = new WhereParser(new SqlParser(), conditionExpr);
            /*
             * 将case when的各种条件判断转换为script的if-else判断，举例如下
             * case when：
             *    CASE
             *    WHEN platform_id = 'PC' AND os NOT IN ('全部') THEN 'unknown'
             *    ELSE os
             * script的if-else：
             *    将上文case when例子中的WHEN platform_id = 'PC' AND os NOT IN ('全部') THEN 'unknown' 解析成如下的script：
             *    (doc['platform_id'].value=='PC') && (doc['os'].value != '全部' )
             */
            String scriptCode = explain(parser.findWhere());
            if (scriptCode.startsWith(" &&")) {
                scriptCode = scriptCode.substring(3);
            }
            if (result.size() == 0) {
                result.add("if(" + scriptCode + ")" + "{" + ParseUtils.getScriptValueWithQuote(item.getValueExpr(), "'") + "}");
            } else {
                result.add("else if(" + scriptCode + ")" + "{" + ParseUtils.getScriptValueWithQuote(item.getValueExpr(), "'") + "}");
            }

        }
        SQLExpr elseExpr = caseExpr.getElseExpr();
        if (elseExpr == null) {
            result.add("else { null }");
        } else {
            result.add("else {" + ParseUtils.getScriptValueWithQuote(elseExpr, "'") + "}");
        }

        return Joiner.on(" ").join(result);
    }

    /**
     * 将where子句中的case when解析为es script
     * 这种情况的es script和select、group by、order by等子句中的case when的es script不一样
     * 因为在where子句中script的返回值是布尔类型，所以script中需要有个布尔判断
     * 而其他情况的script返回值就是转换后的值，该值一般是字符串、数值
     */
    public String parseCaseWhenInWhere(Object[] valueArr) {
        List<String> result = new ArrayList<>();
        String TMP = "tmp";
        result.add("String " + TMP + " = '';");

        for (SQLCaseExpr.Item item : caseExpr.getItems()) {
            SQLExpr conditionExpr = item.getConditionExpr();

            WhereParser parser = new WhereParser(new SqlParser(), conditionExpr);

            String scriptCode = explain(parser.findWhere());
            if (scriptCode.startsWith(" &&")) {
                scriptCode = scriptCode.substring(3);
            }
            // 在for循环之前就已经先add了一个元素 ==1表示第一次
            if (result.size() == 1) {
                result.add("if(" + scriptCode + ")" + "{" + TMP + "=" + ParseUtils.getScriptValueWithQuote(item.getValueExpr(), "'") + "}");
            } else {
                result.add("else if(" + scriptCode + ")" + "{" + TMP + "=" + ParseUtils.getScriptValueWithQuote(item.getValueExpr(), "'") + "}");
            }

        }
        SQLExpr elseExpr = caseExpr.getElseExpr();
        if (elseExpr == null) {
            result.add("else { null }");
        } else {
            result.add("else {" + TMP + "=" + ParseUtils.getScriptValueWithQuote(elseExpr, "'") + "}");
        }

        /*
         * 1、第一种情况in
         * field in (a, b, c)     --> field == a || field == b || field == c
         * 2、第二种情况not in
         * field not in (a, b, c) --> field != a && field != b && field != c
         *                  等价于 --> !(field == a || field == b || field == c) 即对第一种情况取反，
         *                             (field == a || field == b || field == c)里的a、b、c要全部为false，!(field == a || field == b || field == c)才为true
         * 3、这里只拼接第一种情况，不拼接第二种情况，如果要需要第二种情况，那就调用该方法得到返回值后自行拼上取反符号和括号: !(${该方法的返回值})
         */
        String judgeStatement = parseInNotInJudge(valueArr, TMP);
        result.add("return " + judgeStatement + ";");
        return Joiner.on(" ").join(result);
    }

    /**
     * 将case when的各种条件判断转换为script的if-else判断，举例如下
     * case when：
     * CASE
     * WHEN platform_id = 'PC' AND os NOT IN ('全部') THEN 'unknown'
     * ELSE os
     * <p>
     * script的if-else：
     * 将上文case when例子中的WHEN platform_id = 'PC' AND os NOT IN ('全部') THEN 'unknown' 解析成如下的script：
     * (doc['platform_id'].value=='PC') && (doc['os'].value != '全部' )
     */
    public String explain(Where where) {
        List<String> codes = new ArrayList<>();
        while (where.getWheres().size() == 1) {
            where = where.getWheres().getFirst();
        }
        explainWhere(codes, where);
        String relation = where.getConn().name().equals("AND") ? " && " : " || ";
        return Joiner.on(relation).join(codes);
    }

    private void explainWhere(List<String> codes, Where where) {
        if (where instanceof Condition) {
            Condition condition = (Condition) where;

            if (condition.getValue() instanceof ScriptFilter) {
                codes.add(String.format("Function.identity().compose((o)->{%s}).apply(null)", ((ScriptFilter) condition.getValue()).getScript()));
            } else if (condition.getEsOperator() == ESOperator.BETWEEN) {
                Object[] objs = (Object[]) condition.getValue();
                codes.add("(" + "doc['" + condition.getName() + "'].value >= " + objs[0] + " && doc['" + condition.getName() + "'].value <=" + objs[1] + ")");
            } else if (condition.getEsOperator() == ESOperator.IN) { // in
                // 解析case when判断语句中的in、not in判断语句
                codes.add(parseInNotInJudge(condition, "==", "||", false));
            } else if (condition.getEsOperator() == ESOperator.NIN) { // not in
                codes.add(parseInNotInJudge(condition, "!=", "&&", false));
            } else {
                SQLExpr nameExpr = condition.getNameExpr();
                SQLExpr valueExpr = condition.getValueExpr();
                if (valueExpr instanceof SQLNullExpr) {
                    // 空值查询
                    codes.add("(" + "doc['" + nameExpr.toString() + "']" + ".empty)");
                } else {
                    // (doc['c'].value==1)
                    codes.add("(" + ParseUtils.getScriptValueWithQuote(nameExpr, "'") + condition.getOperatorSymbol() + ParseUtils.getScriptValueWithQuote(valueExpr, "'") + ")");
                }
            }
        } else {
            for (Where subWhere : where.getWheres()) {
                List<String> subCodes = new ArrayList<>();
                explainWhere(subCodes, subWhere);
                String relation = subWhere.getConn().name().equals("AND") ? "&&" : "||";
                codes.add(Joiner.on(relation).join(subCodes));
            }
        }
    }

    private String parseInNotInJudge(Condition condition, String judgeOperator, String booleanOperator, boolean flag) {
        Object[] objArr = (Object[]) condition.getValue();
        if (objArr.length == 0)
            throw new SqlParseException("you should assign some value in bracket!!");

        StringBuilder script = new StringBuilder("(");

        // 结尾这个空格就只空一格
        String template = "doc['" + condition.getName() + "'].value " + judgeOperator + " %s " + booleanOperator + " ";
        if (flag) {
            // 结尾这个空格就只空一格
            template = condition.getName() + " " + judgeOperator + " %s " + booleanOperator + " ";
        }
        for (Object obj : objArr) {
            script.append(String.format(template, parseInNotInValueWithQuote(obj)));
        }
        // 去掉末尾的&&
        script = new StringBuilder(script.substring(0, script.lastIndexOf(booleanOperator)));
        // script结果示例 (doc['a'].value == 1 && doc['a'].value == 2 && doc['a'].value == 3 )
        script.append(")");
        return script.toString();

    }

    private String parseInNotInJudge(Object value, String fieldName) {
        Condition condition = new Condition(null);
        condition.setName(fieldName);
        condition.setValue(value);
        return parseInNotInJudge(condition, "==", "||", true);
    }

    private Object parseInNotInValueWithQuote(Object obj) {
        // TODO 可能还需要判断Date等复杂类型
        if (obj instanceof String) {
            return "'" + obj + "'";
        } else {
            return obj;
        }
    }
}
