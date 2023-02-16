package com.mogudiandian.elasticsearch.orm.core.request.sql.core;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.util.StringUtils;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.KVValue;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Created by allwefantasy on 8/19/16.
 */
public class SQLFunctions {

    // Groovy Built In Functions
    public final static Set<String> buildInFunctions = Sets.newHashSet(
            "exp", "log", "log2", "log10", "log10", "sqrt", "cbrt", "ceil", "floor", "rint", "pow", "round", "random", "abs", // nummber operator
            "split", "concat_ws", "substring", "trim", // string operator
            "add", "multiply", "divide", "subtract", "modulus", // binary operator
            "field", "date_format", "if", // if判断目前支持多个二元操作符
            "max_bw", "min_bw", // 取两个数的最大/小值
            "coalesce", // 取两个值中间有值的那个
            "case_new", // 支持多个判断条件
            // 支持正则表达式抽取原字段后赋给新字段,注意必须指定一个group。如 parse(hobby,(?<type>\S+)球, defaultValue)
            "parse" // 此函数需要在elasticsearch.yml中设置 script.painless.regex.enabled : true
    );

    // 增加二元操作运算符
    public static Set<String> binaryOperators = Sets.newHashSet("=", "!=", ">", ">=", "<", "<=");

    // 增加 binaryOperatorName，即 if、case条件中的判断
    public static Tuple<String, String> function(String methodName, List<KVValue> parameters, String name, boolean returnValue, String binaryOperatorName, List<String> binaryOperatorNames) throws Exception {
        // 默认二元操作符为 ==
        if (binaryOperatorName == null || binaryOperatorName.equals("=")) {
            binaryOperatorName = " == ";
        }

        Tuple<String, String> functionStr = null;
        switch (methodName.toLowerCase()) {
            case "if":
                StringBuilder caseString = new StringBuilder();
                StringBuilder nameIF = new StringBuilder();
                if (parameters.get(0).getValue() instanceof SQLInListExpr) {
                    nameIF.append(methodName).append("(").append(((SQLInListExpr) parameters.get(0).getValue()).getExpr()).append(" in (");
                    String left = "doc['" + ((SQLInListExpr) parameters.get(0).getValue()).getExpr().toString() + "'].getValue()";
                    List<SQLExpr> targetList = ((SQLInListExpr) parameters.get(0).getValue()).getTargetList();
                    for (SQLExpr a : targetList) {
                        caseString.append(left).append(" == '").append(a.toString()).append("' ||");
                        nameIF.append(a.toString()).append(",");
                    }
                    caseString = new StringBuilder(caseString.substring(0, caseString.length() - 2));
                    nameIF = new StringBuilder(nameIF.substring(0, nameIF.length() - 1) + "),");
                } else {
                    String key = parameters.get(0).getKey();
                    String left = "doc['" + key + "'].getValue()";
                    String value = parameters.get(0).getValue().toString();
                    // 支持更多的表达式，如 > 、<、>=、<=、!= 等
                    caseString.append(left).append(binaryOperatorName).append(value);
                    nameIF = new StringBuilder(methodName + "(" + key + binaryOperatorName + value + ",");
                }
                nameIF.append(parameters.get(1).getValue()).append(",").append(parameters.get(2).getValue()).append(")");
                functionStr = new Tuple<>(nameIF.toString(), "if((" + caseString + ")){" + parameters.get(1).getValue() + "} else {" + parameters.get(2).getValue() + "}");
                break;
            case "split":
                if (parameters.size() == 3) {
                    functionStr = split(ParseUtils.expr2Object((SQLExpr) parameters.get(0).getValue()).toString(),
                            ParseUtils.expr2Object((SQLExpr) parameters.get(1).getValue()).toString(),
                            Integer.parseInt(ParseUtils.expr2Object((SQLExpr) parameters.get(2).getValue()).toString()), name);
                } else {
                    functionStr = split(parameters.get(0).getValue().toString(), parameters.get(1).getValue().toString(), name);
                }
                break;
            case "concat_ws":
                List<SQLExpr> result = Lists.newArrayList();
                for (int i = 1; i < parameters.size(); i++) {
                    result.add((SQLExpr) parameters.get(i).getValue());
                }
                functionStr = concatWs(parameters.get(0).getValue().toString(), result);
                break;
            case "date_format":
                functionStr = date_format(ParseUtils.expr2Object((SQLExpr) parameters.get(0).getValue()).toString(),
                        ParseUtils.expr2Object((SQLExpr) parameters.get(1).getValue()).toString(),
                        2 < parameters.size() ? ParseUtils.expr2Object((SQLExpr) parameters.get(2).getValue()).toString() : null,
                        name);
                break;
            case "abs":
            case "round":
            case "max_bw":
            case "min_bw":
            case "coalesce":
            case "parse":
            case "case_new":
            case "floor":
                // es的round()默认是保留到个位，这里给round()函数加上精确到小数点后第几位的功能
                // 增加两个函数 min_bw 和 max_bw
                if (parameters.size() >= 2) { // coalesce函数的参数可以是2个以上
                    if (methodName.equals("round")) {
                        int decimalPrecision = Integer.parseInt(parameters.get(1).getValue().toString());
                        functionStr = mathRoundTemplate("Math." + methodName, methodName, ParseUtils.expr2Object((SQLExpr) parameters.get(0).getValue()).toString(), name, decimalPrecision);
                        break;
                    } else if (methodName.equals("max_bw")) {
                        functionStr = mathBetweenTemplate("Math.max", methodName, parameters);
                        break;
                    } else if (methodName.equals("min_bw")) {
                        functionStr = mathBetweenTemplate("Math.min", methodName, parameters);
                        break;
                    } else if (methodName.equals("coalesce")) {
                        functionStr = coalesceTemplate(methodName, parameters);
                        break;
                    } else if (methodName.equals("case_new")) {
                        functionStr = caseNewTemplate(methodName, parameters, binaryOperatorNames);
                        break;
                    } else if (methodName.equals("parse")) {
                        functionStr = parseTemplate(methodName, parameters);
                        break;
                    }
                }
            case "ceil":
            case "cbrt":
            case "rint":
            case "exp":
            case "sqrt":
                functionStr = mathSingleValueTemplate("Math." + methodName, methodName, ParseUtils.expr2Object((SQLExpr) parameters.get(0).getValue()).toString(), name);
                break;
            case "pow":
                functionStr = mathDoubleValueTemplate("Math." + methodName, methodName, ParseUtils.expr2Object((SQLExpr) parameters.get(0).getValue()).toString(), ParseUtils.expr2Object((SQLExpr) parameters.get(1).getValue()).toString(), name);
                break;
            case "substring":
                functionStr = substring(ParseUtils.expr2Object((SQLExpr) parameters.get(0).getValue()).toString(),
                        Integer.parseInt(ParseUtils.expr2Object((SQLExpr) parameters.get(1).getValue()).toString()),
                        Integer.parseInt(ParseUtils.expr2Object((SQLExpr) parameters.get(2).getValue()).toString())
                        , name);
                break;
            case "trim":
                functionStr = trim(ParseUtils.expr2Object((SQLExpr) parameters.get(0).getValue()).toString(), name);
                break;
            case "add":
                functionStr = add((SQLExpr) parameters.get(0).getValue(), (SQLExpr) parameters.get(1).getValue());
                break;
            case "subtract":
                functionStr = subtract((SQLExpr) parameters.get(0).getValue(), (SQLExpr) parameters.get(1).getValue());
                break;
            case "divide":
                functionStr = divide((SQLExpr) parameters.get(0).getValue(), (SQLExpr) parameters.get(1).getValue());
                break;
            case "multiply":
                functionStr = multiply((SQLExpr) parameters.get(0).getValue(), (SQLExpr) parameters.get(1).getValue());
                break;
            case "modulus":
                functionStr = modulus((SQLExpr) parameters.get(0).getValue(), (SQLExpr) parameters.get(1).getValue());
                break;
            case "field":
                functionStr = field(ParseUtils.expr2Object((SQLExpr) parameters.get(0).getValue()).toString());
                break;
            case "log2":
                functionStr = log(SQLUtils.toSQLExpr("2"), (SQLExpr) parameters.get(0).getValue(), name);
                break;
            case "log10":
                functionStr = log(SQLUtils.toSQLExpr("10"), (SQLExpr) parameters.get(0).getValue(), name);
                break;
            case "log":
                List<SQLExpr> logs = Lists.newArrayList();
                for (int i = 0; i < parameters.size(); i++) {
                    logs.add((SQLExpr) parameters.get(0).getValue());
                }
                if (logs.size() > 1) {
                    functionStr = log(logs.get(0), logs.get(1), name);
                } else {
                    functionStr = log(SQLUtils.toSQLExpr("Math.E"), logs.get(0), name);
                }
                break;
            default:
                break;
        }

        // 以下几种情况的脚本，script中均不需要return语句
        if (returnValue && !methodName.equalsIgnoreCase("if")
                && !methodName.equalsIgnoreCase("coalesce")
                && !methodName.equalsIgnoreCase("parse")
                && !methodName.equalsIgnoreCase("case_new")
                && buildInFunctions.contains(methodName)) {
            String generatedFieldName = functionStr.v1();
            String returnCommand = ";return " + generatedFieldName + ";";
            String newScript = functionStr.v2() + returnCommand;
            functionStr = new Tuple<>(generatedFieldName, newScript);
        }
        return functionStr;
    }

    public static String random() {
        return Math.abs(new Random().nextInt()) + "";
    }

    private static Tuple<String, String> concatWs(String split, List<SQLExpr> columns) {
        String name = "concat_ws_" + random();
        List<String> result = Lists.newArrayList();

        for (SQLExpr column : columns) {
            String strColumn = ParseUtils.expr2Object(column).toString();
            if (strColumn.startsWith("def ")) {
                result.add(strColumn);
            } else if (isProperty(column)) {
                result.add("doc['" + strColumn + "'].getValue()");
            } else {
                result.add("'" + strColumn + "'");
            }
        }
        return new Tuple<>(name, "def " + name + " =" + Joiner.on("+ " + split + " +").join(result));
    }

    // split(Column str, java.lang.String pattern)
    public static Tuple<String, String> split(String strColumn, String pattern, int index, String valueName) {
        String name = "split_" + random();
        String script;
        if (valueName == null) {
            script = "def " + name + " = doc['" + strColumn + "'].getValue().split('" + pattern + "')[" + index + "]";

        } else {
            script = "; def " + name + " = " + valueName + ".split('" + pattern + "')[" + index + "]";
        }
        return new Tuple<>(name, script);
    }

    private static Tuple<String, String> date_format(String strColumn, String pattern, String zoneId, String valueName) {
        String name = "date_format_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = DateTimeFormatter.ofPattern('" + pattern + "').withZone(" +
                    (zoneId != null ? "ZoneId.of('" + zoneId + "')" : "ZoneId.systemDefault()") +
                    ").format(Instant.ofEpochMilli(doc['" + strColumn + "'].getValue().getMillis()))");
        } else {
            return new Tuple<>(name, strColumn + "; def " + name + " = new SimpleDateFormat('" + pattern + "').format(new Date(" + valueName + " - 8*1000*60*60))");
        }
    }

    public static Tuple<String, String> add(SQLExpr a, SQLExpr b) {
        return binaryOpertator("add", "+", a, b);
    }

    private static Tuple<String, String> modulus(SQLExpr a, SQLExpr b) {
        return binaryOpertator("modulus", "%", a, b);
    }

    public static Tuple<String, String> field(String a) {
        String name = "field_" + random();
        return new Tuple<>(name, "def " + name + " = " + "doc['" + a + "'].getValue()");
    }

    private static Tuple<String, String> subtract(SQLExpr a, SQLExpr b) {
        return binaryOpertator("subtract", "-", a, b);
    }

    private static Tuple<String, String> multiply(SQLExpr a, SQLExpr b) {
        return binaryOpertator("multiply", "*", a, b);
    }

    private static Tuple<String, String> divide(SQLExpr a, SQLExpr b) {
        return binaryOpertator("divide", "/", a, b);
    }

    private static Tuple<String, String> binaryOpertator(String methodName, String operator, SQLExpr a, SQLExpr b) {
        String name = methodName + "_" + random();
        return new Tuple<>(name,
                scriptDeclare(a) + scriptDeclare(b) +
                        convertType(a) + convertType(b) +
                        " def " + name + " = " + extractName(a) + " " + operator + " " + extractName(b));
    }

    private static boolean isProperty(SQLExpr expr) {
        return (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr);
    }

    private static String scriptDeclare(SQLExpr a) {
        if (isProperty(a) || a instanceof SQLNumericLiteralExpr) {
            return "";
        }
        return ParseUtils.expr2Object(a).toString() + ";";
    }

    private static String extractName(SQLExpr script) {
        if (isProperty(script)) {
            return "doc['" + script + "'].getValue()";
        }
        String scriptStr = ParseUtils.expr2Object(script).toString();
        String[] variance = scriptStr.split(";");
        String newScript = variance[variance.length - 1];
        if (newScript.trim().startsWith("def ")) {
            // for now ,if variant is string,then change to double.
            return newScript.trim().substring(4).split("=")[0].trim();
        }
        return scriptStr;
    }

    // cast(year as int)
    private static String convertType(SQLExpr script) {
        String[] variance = ParseUtils.expr2Object(script).toString().split(";");
        String newScript = variance[variance.length - 1];
        if (newScript.trim().startsWith("def ")) {
            // for now ,if variant is string,then change to double.
            String temp = newScript.trim().substring(4).split("=")[0].trim();
            return " if( " + temp + " instanceof String) " + temp + "= Double.parseDouble(" + temp.trim() + "); ";
        }
        return "";
    }

    public static Tuple<String, String> log(String strColumn, String valueName) {
        return mathSingleValueTemplate("log", strColumn, valueName);
    }

    public static Tuple<String, String> log10(String strColumn, String valueName) {
        return mathSingleValueTemplate("log10", strColumn, valueName);
    }

    public static Tuple<String, String> log(SQLExpr base, SQLExpr strColumn, String valueName) {
        String name = "log_" + random();
        String result;
        if (valueName == null) {
            if (isProperty(strColumn)) {
                result = "def " + name + " = Math.log(doc['" + ParseUtils.expr2Object(strColumn).toString() + "'].getValue())/Math.log(" + ParseUtils.expr2Object(base).toString() + ")";
            } else {
                result = "def " + name + " = Math.log(" + ParseUtils.expr2Object(strColumn).toString() + ")/Math.log(" + ParseUtils.expr2Object(base).toString() + ")";
            }
        } else {
            result = ParseUtils.expr2Object(strColumn).toString() + ";def " + name + " = Math.log(" + valueName + ")/Math.log(" + ParseUtils.expr2Object(base).toString() + ")";
        }
        return new Tuple(name, result);
    }

    public static Tuple<String, String> sqrt(String strColumn, String valueName) {
        return mathSingleValueTemplate("Math.sqrt", "sqrt", strColumn, valueName);
    }

    public static Tuple<String, String> round(String strColumn, String valueName) {
        return mathSingleValueTemplate("Math.round", "round", strColumn, valueName);
    }

    public static Tuple<String, String> trim(String strColumn, String valueName) {
        return strSingleValueTemplate("trim", strColumn, valueName);
    }

    private static Tuple<String, String> mathDoubleValueTemplate(String methodName, String fieldName, String val1, String val2, String valueName) {
        String name = fieldName + "_" + random();
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = " + methodName + "(doc['" + val1 + "'].getValue(), " + val2 + ")");
        } else {
            return new Tuple(name, val1 + ";def " + name + " = " + methodName + "(" + valueName + ", " + val2 + ")");
        }
    }

    private static Tuple<String, String> mathSingleValueTemplate(String methodName, String strColumn, String valueName) {
        return mathSingleValueTemplate(methodName, methodName, strColumn, valueName);
    }

    private static Tuple<String, String> mathSingleValueTemplate(String methodName, String fieldName, String strColumn, String valueName) {
        String name = fieldName + "_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = " + methodName + "(doc['" + strColumn + "'].getValue())");
        } else {
            return new Tuple<>(name, strColumn + ";def " + name + " = " + methodName + "(" + valueName + ")");
        }

    }

    private static Tuple<String, String> mathRoundTemplate(String methodName, String fieldName, String strColumn, String valueName, int decimalPrecision) {
        StringBuilder builder = new StringBuilder("1");
        for (int i = 0; i < decimalPrecision; i++) {
            builder.append("0");
        }
        double num = Double.parseDouble(builder.toString());

        String name = fieldName + "_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = " + methodName + "((doc['" + strColumn + "'].getValue()) * " + num + ")/" + num);
        } else {
            return new Tuple<>(name, strColumn + ";def " + name + " = " + methodName + "((" + valueName + ") * " + num + ")/" + num);
        }
    }

    // 求两个值中最大值，如 def abs_775880898 = Math.max(doc['age1'].getValue(), doc['age2'].getValue());return abs_775880898;
    private static Tuple<String, String> mathBetweenTemplate(String methodName, String fieldName, List<KVValue> parameter) {
        // 获取 max_bw/min_bw 函数的两个字段
        String name = fieldName + "_" + random();
        StringBuilder builder = new StringBuilder();
        builder.append("def " + name + " = " + methodName + "(");
        int i = 0;
        for (KVValue kv : parameter) {
            String field = kv.getValue().toString();
            if (i > 0) {
                builder.append(", ");
            }
            builder.append("doc['" + field + "'].getValue()");
            i++;
        }
        builder.append(")");
        return new Tuple<>(name, builder.toString());
    }

    // 实现coalesce(field1, field2, ...)功能，只要任意一个不为空即可
    private static Tuple<String, String> coalesceTemplate(String fieldName, List<KVValue> parameter) {
        // if((doc['age2'].getValue() != null)){doc['age2'].getValue()} else if((doc['age1'].getValue() != null)){doc['age1'].getValue()}
        String name = fieldName + "_" + random();
        StringBuilder builder = new StringBuilder();
        int i = 0;
        // sb.append("def " + name + " = ");
        for (KVValue kv : parameter) {
            String field = kv.getValue().toString();
            if (i > 0) {
                builder.append(" else ");
            }
            builder.append("if(doc['" + field + "'].getValue() != null){doc['" + field + "'].getValue()}");
            i++;
        }
        return new Tuple<>(name, builder.toString());
    }

    // 实现正则表达式抽取原字段后赋给新字段,注意必须指定一个group。如 parse(hobby,(?<type>\S+)球, defaultValue)
    // "SELECT  parse(hobby, '(?<type>\\\\S+)球', 'NOT_MATCH') AS ballType, COUNT(_index) FROM bank GROUP BY ballType"
    private static Tuple<String, String> parseTemplate(String fieldName, List<KVValue> params) {
        //  def m = /(?<type>\S+)球/.matcher(doc['hobby'].getValue()); if(m.matches()) { return m.group(1) } else { return \"no_match\" }
        String name = fieldName + "_" + random();
        StringBuilder builder = new StringBuilder();
        if (null == params || params.size() != 3) {
            throw new IllegalArgumentException("parse 函数必须包含三个参数，第一个是原字段，第二个是带有group的正则表达式, 第三个是抽取不成功的默认值");
        }
        String srcField = params.get(0).getValue().toString();
        String regexStr = params.get(1).getValue().toString();
        // 需要去除自动添加的单引号
        regexStr = regexStr.substring(1, regexStr.length() - 1);
        String defaultValue = params.get(2).getValue().toString();

        builder.append("def m = /" + regexStr + "/.matcher(doc['" + srcField + "'].getValue()); if(m.matches()) { return m.group(1) } else { return " + defaultValue + " }");
        return new Tuple<>(name, builder.toString());
    }

    // 实现 case_new(gender='m', '男', gender='f', '女',  default, '无') as myGender  功能
    private static Tuple<String, String> caseNewTemplate(String fieldName, List<KVValue> parameter, List<String> binaryOperatorNames) {
        // 如果参数不是偶数个，则抛异常
        if (parameter.size() % 2 != 0) {
            throw new IllegalArgumentException("请检查参数数量，必须是偶数个！");
        }
        // 找出所有字段及其对应的值存入到Map中，如果有default，则将其移除
        String defaultVal = null;
        List<String> fieldList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        List<Object> defaultList = new ArrayList<>();
        for (int i = 0; i < parameter.size(); i = i + 2) {
            String _default = parameter.get(i + 1).getValue().toString();
            // 记录默认值
            if (parameter.get(i).getValue().toString().equalsIgnoreCase("default")) {
                defaultVal = _default;
            } else {
                fieldList.add(parameter.get(i).getKey());
                valueList.add(parameter.get(i).getValue().toString());
                defaultList.add(_default);
            }
        }
        //  if((doc['gender'].getValue() == 'm')) '男' else if((doc['gender'].getValue() == 'f')) '女' else ''无
        String name = fieldName + "_" + random();
        StringBuilder builder = new StringBuilder();
        int i = 0;
        for (int j = 0; j < fieldList.size(); j++) {
            String field = fieldList.get(j);
            if (i > 0) {
                builder.append(" else ");
            }
            // 此处有问题，还需要支持除 == 外的其他二元操作符
            // sb.append("if(doc['" + field + "'].getValue() == " + valueList.get(i) + ") { " + defaultList.get(i) + " }");
            String binaryOperatorName = binaryOperatorNames.get(j);
            if ("=".equals(binaryOperatorName)) { // SQL中只有 = 符号，但script中必须使用 ==
                binaryOperatorName = "==";
            }
            builder.append("if(doc['" + field + "'].getValue() " + binaryOperatorName + " " + valueList.get(i) + ") { " + defaultList.get(i) + " }");
            i++;
        }
        if (!StringUtils.isEmpty(defaultVal)) {
            builder.append(" else " + defaultVal);
        }
        return new Tuple<>(name, builder.toString());
    }

    public static Tuple<String, String> strSingleValueTemplate(String methodName, String strColumn, String valueName) {
        String name = methodName + "_" + random();
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = doc['" + strColumn + "'].getValue()." + methodName + "()");
        } else {
            return new Tuple(name, strColumn + "; def " + name + " = " + valueName + "." + methodName + "()");
        }
    }

    public static Tuple<String, String> floor(String strColumn, String valueName) {
        return mathSingleValueTemplate("Math.floor", "floor", strColumn, valueName);
    }

    // substring(Column str, int pos, int len)
    public static Tuple<String, String> substring(String strColumn, int pos, int len, String valueName) {
        String name = "substring_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = doc['" + strColumn + "'].getValue().substring(" + pos + "," + len + ")");
        } else {
            return new Tuple<>(name, strColumn + ";def " + name + " = " + valueName + ".substring(" + pos + "," + len + ")");
        }
    }

    // split(Column str, java.lang.String pattern)
    public static Tuple<String, String> split(String strColumn, String pattern, String valueName) {
        String name = "split_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = doc['" + strColumn + "'].getValue().split('" + pattern + "')");
        } else {
            return new Tuple<>(name, strColumn + "; def " + name + " = " + valueName + ".split('" + pattern + "')");
        }
    }

}
