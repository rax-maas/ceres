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
    private Integer limit;
    private List<SeriesData> results;
    private MetricNameAndMultiTags metricAndTags;

    @Autowired
    public LookupApiController(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    @GetMapping("/lookup")
    public Mono<LookupResult> query(@RequestParam(name = "m") String m,
                                    @RequestParam(name = "limit", required = false) Integer limit,
                                    @RequestHeader(value = "X-Tenant") String tenantHeader) {

        this.limit = limit;
        this.results = new ArrayList<>();
        this.metricAndTags = metadataService.getMetricNameAndTags(m);

        return metadataService.getTagKeysMaps(tenantHeader, metricAndTags.getMetricName())
                .flatMapMany(Flux::fromIterable)
                .flatMap(tagKeyMap -> metadataService.getTagValueMaps(tagKeyMap)
                        .flatMapMany(Flux::fromIterable)
                        .flatMap(this::handleTagValue)
                ).then(this.getResult());
    }

    private Mono<String> handleTagValue(Map<String, String> tag) {
        String tagKey = tag.get("tagKey");
        String tagValue = tag.get("tagValue");
        if (this.metricAndTags.getTags().isEmpty()) {
            if (this.limit == null || this.results.size() < this.limit) {
                this.results.add(new SeriesData().setTags(Map.of(tagKey, tagValue)));
            }
        } else {
            this.metricAndTags.getTags().forEach(tagItem -> {
                Map.Entry<String, String> entry = tagItem.entrySet().iterator().next();
                String k = entry.getKey();
                String v = entry.getValue();
                if ((tagKey.equals(k) && tagValue.equals(v)) ||
                        (tagKey.equals(k) && v.equals("*")) ||
                        (k.equals("*") && tagValue.equals(v))) {
                    if (this.limit == null || results.size() < this.limit) {
                        this.results.add(new SeriesData().setTags(Map.of(tagKey, tagValue)));
                    }
                }
            });
        }
        return Mono.just("");
    }

    private Mono<LookupResult> getResult() {
        LookupResult result = new LookupResult()
                .setType("LOOKUP")
                .setMetric(this.metricAndTags.getMetricName())
                .setLimit(this.limit)
                .setResults(this.results);
        return Mono.just(result);
    }
}
