package com.rackspace.ceres.app.model;

import java.util.List;
import lombok.Data;

@Data
public class LookupResult {
    String type;
    String metric;
    Integer limit;
    List<SeriesData> results;
}
