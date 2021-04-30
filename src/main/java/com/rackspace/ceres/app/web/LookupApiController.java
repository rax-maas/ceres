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
 * Native Ceres lookup API endpoints.
 */
@RestController
@RequestMapping("/api/search")
@Profile("query")
public class LookupApiController {
    private final MetadataService metadataService;
    private final static String tagValueRegex = ".*\\{.*\\}$";

    @Autowired
    public LookupApiController(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    @GetMapping("/lookup")
    public Mono<LookupResult> query(@RequestParam(name = "m") String m,
                                    @RequestParam(name = "limit", required = false) Integer limit,
                                    @RequestHeader(value = "X-Tenant", required = true) String tenantHeader) {

        List<SeriesData> results = new ArrayList<>();
        final MetricNameAndMultiTags metricAndTags = metadataService.getMetricNameAndTags(m);

        return metadataService.getTagKeys(tenantHeader, metricAndTags.getMetricName())
                .flatMapMany(Flux::fromIterable)
                .flatMap(tagKey ->
                        metadataService.getTagValues(tenantHeader, metricAndTags.getMetricName(), tagKey)
                                .flatMapMany(Flux::fromIterable)
                                .flatMap(tagValue -> {
                                            handleTagValue(limit, tagKey, tagValue, metricAndTags.getTags(), results);
                                            return Mono.just("");
                                        }
                                )
                ).then(getResult(results, metricAndTags.getMetricName(), limit));
    }

    private void handleTagValue(
            Integer limit, String tagKey, String tagValue, List<Map<String, String>> tags, List<SeriesData> results) {
        if (tags.isEmpty()) {
            if (limit == null || results.size() < limit) {
                results.add(new SeriesData().setTags(Map.of(tagKey, tagValue)));
            }
        } else {
            tags.forEach(tag -> {
                Map.Entry<String, String> entry = tag.entrySet().iterator().next();
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
