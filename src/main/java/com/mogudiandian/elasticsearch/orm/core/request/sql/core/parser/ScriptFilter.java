package com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.ParseUtils;
import org.elasticsearch.script.ScriptType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eliran on 11/12/2015.
 */
public class ScriptFilter {

    private String script;

    private Map<String, Object> args;

    private ScriptType scriptType;

    public ScriptFilter() {
        scriptType = ScriptType.INLINE;
    }

    public boolean tryParseFromMethodExpr(SQLMethodInvokeExpr expr) {
        if (!expr.getMethodName().toLowerCase().equals("script")) {
            return false;
        }
        List<SQLExpr> methodParameters = expr.getParameters();
        if (methodParameters.size() == 0) {
            return false;
        }
        script = ParseUtils.extendedToString(methodParameters.get(0));

        if (methodParameters.size() == 1) {
            return true;
        }

        args = new HashMap<>();

        for (int i = 1; i < methodParameters.size(); i++) {
            SQLExpr innerExpr = methodParameters.get(i);
            if (!(innerExpr instanceof SQLBinaryOpExpr)) {
                return false;
            }
            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) innerExpr;
            if (!binaryOpExpr.getOperator().getName().equals("=")) {
                return false;
            }

            SQLExpr right = binaryOpExpr.getRight();
            Object value = ParseUtils.expr2Object(right);
            String key = ParseUtils.extendedToString(binaryOpExpr.getLeft());
            if (key.equals("script_type")) {
                parseAndUpdateScriptType(value.toString());
            } else {
                args.put(key, value);
            }

        }
        return true;
    }

    private void parseAndUpdateScriptType(String scriptType) {
        String scriptTypeUpper = scriptType.toUpperCase();
        switch (scriptTypeUpper) {
            case "INLINE":
                this.scriptType = ScriptType.INLINE;
                break;
            case "INDEXED":
            case "STORED":
                this.scriptType = ScriptType.STORED;
                break;
        }
    }

    public boolean containsParameters() {
        return args != null && args.size() > 0;
    }

    public String getScript() {
        return script;
    }

    public ScriptType getScriptType() {
        return scriptType;
    }

    public Map<String, Object> getArgs() {
        return args;
    }

}
