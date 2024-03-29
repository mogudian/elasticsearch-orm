package com.mogudiandian.elasticsearch.orm.core.request.sql.core.spatial;

import java.util.List;

/**
 * Created by Eliran on 15/8/2015.
 */
public class PolygonFilterParams {

    private List<Point> polygon;

    public PolygonFilterParams(List<Point> polygon) {
        this.polygon = polygon;
    }

    public List<Point> getPolygon() {
        return polygon;
    }
}
