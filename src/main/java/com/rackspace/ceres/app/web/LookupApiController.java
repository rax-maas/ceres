package com.rackspace.ceres.app.web;

import com.rackspace.ceres.app.model.LookupResult;
import com.rackspace.ceres.app.model.MetricNameAndMultiTags;
import com.rackspace.ceres.app.model.SeriesData;
import com.rackspace.ceres.app.services.MetadataService;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.*;
import springfox.documentation.annotations.ApiIgnore;

/**
 * Native Ceres lookup API endpoints.
 */
@RestController
@RequestMapping("/api/search")
@Profile("query")
public class LookupApiController {
    private final MetadataService metadataService;
    private List<SeriesData> results;
    private MetricNameAndMultiTags lookupMetricAndTags;

    @Autowired
    public LookupApiController(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    @GetMapping("/lookup")
    @ApiOperation(value = "This api is used  to determine what time series are associated"
        + " with a given metric, tag name, tag value, tag pair or combination thereof")
    public Mono<LookupResult> query(@RequestParam(name = "m") String m,
                                    @RequestParam(name = "limit", required = false) Integer limit,
                                    @ApiIgnore @RequestHeader(value = "#{appProperties.tenantHeader}") String tenantHeader) {
        this.results = new ArrayList<>();
        this.lookupMetricAndTags = metadataService.getMetricNameAndTags(m);

        return metadataService.getTags(tenantHeader, this.lookupMetricAndTags.getMetricName())
                .flatMap(tag -> this.handleTagValue(tag, limit))
                .then(this.getResult(limit));
    }

    private Mono<String> handleTagValue(Map<String, String> tag, Integer limit) {
        if (this.lookupMetricAndTags.getTags().isEmpty()) {
            if (limit == null || this.results.size() < limit) {
                this.results.add(new SeriesData().setTags(tag));
            }
        } else {
            this.lookupMetricAndTags.getTags().forEach(tagItem -> {
                Map.Entry<String, String> tagEntry = tag.entrySet().iterator().next();
                String tagKey = tagEntry.getKey();
                String tagValue = tagEntry.getValue();
                Map.Entry<String, String> entry = tagItem.entrySet().iterator().next();
                String k = entry.getKey();
                String v = entry.getValue();
                if ((tagKey.equals(k) && tagValue.equals(v)) ||
                        (tagKey.equals(k) && v.equals("*")) ||
                        (k.equals("*") && tagValue.equals(v))) {
                    if (limit == null || results.size() < limit) {
                        this.results.add(new SeriesData().setTags(tag));
                    }
                }
            });
        }
        return Mono.just("");
    }

    private Mono<LookupResult> getResult(Integer limit) {
        LookupResult result = new LookupResult()
                .setType("LOOKUP")
                .setMetric(this.lookupMetricAndTags.getMetricName())
                .setLimit(limit)
                .setResults(this.results);
        return Mono.just(result);
    }
}
