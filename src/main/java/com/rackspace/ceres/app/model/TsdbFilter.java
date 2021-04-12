package com.rackspace.ceres.app.model;

import lombok.Data;

@Data
public class TsdbFilter {
    FilterType type;
    String tagk;
    String filter;
}
