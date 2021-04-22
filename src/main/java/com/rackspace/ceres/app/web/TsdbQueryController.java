package com.rackspace.ceres.app.web;

import com.rackspace.ceres.app.model.LookupResult;
import com.rackspace.ceres.app.model.MetricNameAndMultiTags;
import com.rackspace.ceres.app.model.SeriesData;
import com.rackspace.ceres.app.services.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

/**
 * Native Ceres search API endpoints.
 */
@RestController
@RequestMapping("/api/search")
@Profile("query")
public class TsdbQueryController {
    private final MetadataService metadataService;

    @Autowired
    public TsdbQueryController(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    @GetMapping("/lookup")
    public Mono<LookupResult> query(@RequestParam(name = "m") String m,
                                    @RequestParam(name = "limit", required = false) Integer limit,
                                    @RequestHeader(value = "X-Tenant", required = true) String tenantHeader) {

        List<SeriesData> results = new ArrayList<SeriesData>();
        final MetricNameAndMultiTags metricNameAndTags = getMetric(m);

        return metadataService.getTagKeys(tenantHeader, metricNameAndTags.getMetricName())
                .flatMapMany(Flux::fromIterable)
                .flatMap(tagKey ->
                        metadataService.getTagValues(tenantHeader, metricNameAndTags.getMetricName(), tagKey)
                                .flatMapMany(Flux::fromIterable)
                                .flatMap(tagValue ->
                                        handleTagValue(limit, tagKey, tagValue, metricNameAndTags.getTags(), results)
                                )
                ).then(getResult(results, metricNameAndTags.getMetricName(), limit));
    }

    private Mono<String> handleTagValue(
            Integer limit, String tagKey, String tagValue, List<Map<String, String>> tags, List<SeriesData> results) {
        if (tags.isEmpty()) {
            if (limit == null || results.size() < limit) {
                results.add(new SeriesData().setTags(Map.of(tagKey, tagValue)));
            }
        } else {
            tags.forEach(tag -> {
                Map.Entry<String,String> entry = tag.entrySet().iterator().next();
                String k = entry.getKey();
                String v = entry.getValue();
                if ((tagKey.equals(k) && tagValue.equals(v)) ||
                        (tagKey.equals(k) && v.equals("*")) ||
                        (k.equals("*") && tagValue.equals(v))) {
                    if (limit == null || results.size() < limit) {
                        results.add(new SeriesData().setTags(Map.of(tagKey, tagValue)));
                    }
                }
            });
        }
        return Mono.just("");
    }

    private MetricNameAndMultiTags getMetric(String m) {
        MetricNameAndMultiTags metricNameAndTags = new MetricNameAndMultiTags();
        List<Map<String, String>> tags = new ArrayList<Map<String, String>>();
        if (m.matches(".*\\{.*\\}$")) {
            String tagKeys = m.substring(m.indexOf("{") + 1, m.indexOf("}"));
            String metricName = m.split("\\{")[0];
            Arrays.stream(tagKeys.split("\\,")).forEach(tagKey -> {
                String[] splitTag = tagKey.split("\\=");
                tags.add(Map.of(splitTag[0], splitTag[1]));
            });
            metricNameAndTags.setMetricName(metricName);
        } else {
            metricNameAndTags.setMetricName(m);
        }
        metricNameAndTags.setTags(tags);
        return metricNameAndTags;
    }

    private Mono<LookupResult> getResult(List<SeriesData> results, String metric, Integer limit) {
        LookupResult result = new LookupResult()
                .setType("LOOKUP")
                .setMetric(metric)
                .setLimit(limit)
                .setResults(results);
        return Mono.just(result);
    }
}
