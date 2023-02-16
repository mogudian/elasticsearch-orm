package com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.ChildrenType;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.NestedType;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.SqlParseException;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.Setter;

import java.util.Arrays;

/**
 * 过滤条件
 *
 * @author ansj
 */
public class Condition extends Where {

    /**
     * 实体类
     */
    @Setter
    private Class<? extends BaseEntity> entityClass;

    /**
     * 字段名
     */
    private String name;

    private SQLExpr nameExpr;

    private Object value;

    public SQLExpr getNameExpr() {
        return nameExpr;
    }

    public SQLExpr getValueExpr() {
        return valueExpr;
    }

    private SQLExpr valueExpr;

    private ESOperator esOperator;

    private Object relationshipType;

    private boolean isNested;

    private String nestedPath;

    private String innerHits;

    private boolean isChildren;
    private String childType;

    public Condition(CONN conn) {
        super(conn);
    }

    public Condition(CONN conn, String fieldName, SQLExpr nameExpr, String condition, Object obj, SQLExpr valueExpr) {
        this(conn, fieldName, nameExpr, condition, obj, valueExpr, null);
    }

    public Condition(CONN conn, String fieldName, SQLExpr nameExpr, ESOperator condition, Object obj, SQLExpr valueExpr) {
        this(conn, fieldName, nameExpr, condition, obj, valueExpr, null);
    }

    public Condition(CONN conn, String fieldName, SQLExpr nameExpr, String operator, Object value, SQLExpr valueExpr, Object relationshipType) {
        super(conn);

        this.esOperator = null;

        setName(fieldName);

        this.value = value;
        this.nameExpr = nameExpr;
        this.valueExpr = valueExpr;

        this.relationshipType = relationshipType;

        if (this.relationshipType != null) {
            if (this.relationshipType instanceof NestedType) {
                NestedType nestedType = (NestedType) relationshipType;

                this.isNested = true;
                this.nestedPath = nestedType.path;
                this.innerHits = nestedType.getInnerHits();
                this.isChildren = false;
                this.childType = "";
            } else if (relationshipType instanceof ChildrenType) {
                ChildrenType childrenType = (ChildrenType) relationshipType;

                this.isNested = false;
                this.nestedPath = "";
                this.isChildren = true;
                this.childType = childrenType.childType;
            }
        } else {
            this.isNested = false;
            this.nestedPath = "";
            this.isChildren = false;
            this.childType = "";
        }

        // EQ, GT, LT, GTE, LTE, N, LIKE, NLIKE, IS, ISN, IN, NIN
        switch (operator) {
            case "=":
                this.esOperator = esOperator.EQ;
                break;
            case ">":
                this.esOperator = esOperator.GT;
                break;
            case "<":
                this.esOperator = esOperator.LT;
                break;
            case ">=":
                this.esOperator = esOperator.GTE;
                break;
            case "<=":
                this.esOperator = esOperator.LTE;
                break;
            case "<>":
                this.esOperator = esOperator.N;
                break;
            case "LIKE":
                this.esOperator = esOperator.LIKE;
                break;
            case "NOT":
                this.esOperator = esOperator.N;
                break;
            case "NOT LIKE":
                this.esOperator = esOperator.NLIKE;
                break;
            case "IS":
                this.esOperator = esOperator.IS;
                break;
            case "IS NOT":
                this.esOperator = esOperator.ISN;
                break;
            case "NOT IN":
                this.esOperator = esOperator.NIN;
                break;
            case "IN":
                this.esOperator = esOperator.IN;
                break;
            case "BETWEEN":
                this.esOperator = esOperator.BETWEEN;
                break;
            case "NOT BETWEEN":
                this.esOperator = esOperator.NBETWEEN;
                break;
            case "GEO_INTERSECTS":
                this.esOperator = esOperator.GEO_INTERSECTS;
                break;
            case "GEO_BOUNDING_BOX":
                this.esOperator = esOperator.GEO_BOUNDING_BOX;
                break;
            case "GEO_DISTANCE":
                this.esOperator = esOperator.GEO_DISTANCE;
                break;
            case "GEO_POLYGON":
                this.esOperator = esOperator.GEO_POLYGON;
                break;
            case "NESTED":
                this.esOperator = esOperator.NESTED_COMPLEX;
                break;
            case "NOT NESTED":
                this.esOperator = esOperator.NNESTED_COMPLEX;
                break;
            case "CHILDREN":
                this.esOperator = esOperator.CHILDREN_COMPLEX;
                break;
            case "SCRIPT":
                this.esOperator = esOperator.SCRIPT;
                break;
            default:
                throw new SqlParseException(operator + " is err!");
        }
    }

    public Condition(CONN conn, String fieldName, SQLExpr nameExpr, ESOperator oper, Object value, SQLExpr valueExpr, Object relationshipType) {
        super(conn);

        this.esOperator = null;
        this.nameExpr = nameExpr;
        this.valueExpr = valueExpr;

        setName(fieldName);

        this.value = value;
        this.esOperator = oper;
        this.relationshipType = relationshipType;

        if (this.relationshipType != null) {
            if (this.relationshipType instanceof NestedType) {
                NestedType nestedType = (NestedType) relationshipType;

                this.isNested = true;
                this.nestedPath = nestedType.path;
                this.innerHits = nestedType.getInnerHits();
                this.isChildren = false;
                this.childType = "";
            } else if (relationshipType instanceof ChildrenType) {
                ChildrenType childrenType = (ChildrenType) relationshipType;

                this.isNested = false;
                this.nestedPath = "";
                this.isChildren = true;
                this.childType = childrenType.childType;
            }
        } else {
            this.isNested = false;
            this.nestedPath = "";
            this.isChildren = false;
            this.childType = "";
        }
    }

    public String getOperatorSymbol() {
        switch (esOperator) {
            case EQ:
                return "==";
            case GT:
                return ">";
            case LT:
                return "<";
            case GTE:
                return ">=";
            case LTE:
                return "<=";
            case N:
                return "<>";
            case IS:
                return "==";

            case ISN:
                return "!=";
            default:
                throw new SqlParseException(esOperator + " is err!");
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public ESOperator getEsOperator() {
        return esOperator;
    }

    public void setEsOperator(ESOperator esOperator) {
        this.esOperator = esOperator;
    }

    public Object getRelationshipType() {
        return relationshipType;
    }

    public void setRelationshipType(Object relationshipType) {
        this.relationshipType = relationshipType;
    }

    public boolean isNested() {
        return isNested;
    }

    public void setNested(boolean isNested) {
        this.isNested = isNested;
    }

    public String getNestedPath() {
        return nestedPath;
    }

    public void setNestedPath(String nestedPath) {
        this.nestedPath = nestedPath;
    }

    public String getInnerHits() {
        return innerHits;
    }

    public void setInnerHits(String innerHits) {
        this.innerHits = innerHits;
    }

    public boolean isChildren() {
        return isChildren;
    }

    public void setChildren(boolean isChildren) {
        this.isChildren = isChildren;
    }

    public String getChildType() {
        return childType;
    }

    public void setChildType(String childType) {
        this.childType = childType;
    }

    @Override
    public String toString() {
        String result = "";

        if (this.isNested()) {
            result = "nested condition ";
            if (this.getNestedPath() != null) {
                result += "on path:" + this.getNestedPath() + " ";
            }

            if (this.getInnerHits() != null) {
                result += "inner_hits:" + this.getInnerHits() + " ";
            }
        } else if (this.isChildren()) {
            result = "children condition ";

            if (this.getChildType() != null) {
                result += "on child: " + this.getChildType() + " ";
            }
        }

        if (value instanceof Object[]) {
            result += this.conn + " " + this.name + " " + this.esOperator + " " + Arrays.toString((Object[]) value);
        } else {
            result += this.conn + " " + this.name + " " + this.esOperator + " " + this.value;
        }

        return result;
    }

    @Override
    public Object clone() {
        try {
            return new Condition(this.getConn(), this.getName(), this.getNameExpr(), this.getEsOperator(), this.getValue(), this.getValueExpr(), this.getRelationshipType());
        } catch (SqlParseException ignored) {
            return null;
        }
    }
}
