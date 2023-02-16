package com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain;

import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.SqlParseException;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.HashMap;
import java.util.Map;

public enum ESOperator {
    EQ, GT, LT, GTE, LTE, N, LIKE, NLIKE, REGEXP, NREGEXP, IS, ISN, IN, NIN, BETWEEN, NBETWEEN, GEO_INTERSECTS, GEO_BOUNDING_BOX, GEO_DISTANCE, GEO_POLYGON, IN_TERMS, TERM, IDS_QUERY, NESTED_COMPLEX, NNESTED_COMPLEX, CHILDREN_COMPLEX, SCRIPT, NIN_TERMS, NTERM;

    public static Map<String, ESOperator> methodNameToOperator;

    private static BiMap<ESOperator, ESOperator> negatives;

    static {
        methodNameToOperator = new HashMap<>();
        methodNameToOperator.put("term", TERM);
        methodNameToOperator.put("matchterm", TERM);
        methodNameToOperator.put("match_term", TERM);
        methodNameToOperator.put("terms", IN_TERMS);
        methodNameToOperator.put("in_terms", IN_TERMS);
        methodNameToOperator.put("ids", IDS_QUERY);
        methodNameToOperator.put("ids_query", IDS_QUERY);
        methodNameToOperator.put("regexp", REGEXP);
        methodNameToOperator.put("regexp_query", REGEXP);
    }

    static {
        negatives = HashBiMap.create(7);
        negatives.put(EQ, N);
        negatives.put(IN_TERMS, NIN_TERMS);
        negatives.put(TERM, NTERM);
        negatives.put(GT, LTE);
        negatives.put(LT, GTE);
        negatives.put(LIKE, NLIKE);
        negatives.put(IS, ISN);
        negatives.put(IN, NIN);
        negatives.put(BETWEEN, NBETWEEN);
        negatives.put(REGEXP, NREGEXP);
        negatives.put(NESTED_COMPLEX, NNESTED_COMPLEX);
    }

    public ESOperator negative() {
        ESOperator negative = negatives.get(this);
        negative = negative != null ? negative : negatives.inverse().get(this);
        if (negative == null) {
            throw new SqlParseException("OPERATOR negative not supported: " + this);
        }
        return negative;
    }
}
