package com.rackspace.ceres.app.services;

import static org.mockito.Mockito.when;

import com.rackspace.ceres.app.model.SuggestType;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@SpringBootTest(classes = {SuggestApiService.class})
public class SuggestApiServiceTest {

  @Autowired
  SuggestApiService suggestApiService;

  @MockBean
  MetadataService metadataService;

  @Test
  public void testSuggestMetricNames() {

    List<String> metricNames = List.of("cpu_idle", "cpu_process", "process_state", "cpu_busy");
    when(metadataService.getMetricNames("t-1")).thenReturn(Mono.just(metricNames));

    StepVerifier.create(suggestApiService.suggestMetricNames("t-1", "cp", 10))
        .assertNext(metricNameList -> {
          Assertions.assertThat(metricNameList.size()).isEqualTo(3);
          Assertions.assertThat(metricNameList).isEqualTo(List.of("cpu_idle", "cpu_process", "cpu_busy"));
        }).verifyComplete();

    //Test with the max limit
    StepVerifier.create(suggestApiService.suggestMetricNames("t-1", "cp", 2))
        .assertNext(metricNameList -> {
          Assertions.assertThat(metricNameList.size()).isEqualTo(2);
        }).verifyComplete();

    //Test when metric doesn't match with any metric names
    StepVerifier.create(suggestApiService.suggestMetricNames("t-1", "invalid", 10))
        .assertNext(metricNameList -> {
          Assertions.assertThat(metricNameList).isEmpty();
        }).verifyComplete();

    StepVerifier.create(suggestApiService.suggestMetricNames("t-1", "cp", 0))
        .assertNext(metricNameList -> {
          Assertions.assertThat(metricNameList).isEmpty();
        }).verifyComplete();
  }

  @Test
  public void testSuggestTagKeys() {

    List<String> tagKeys = List.of("os", "osx");
    when(metadataService.getTagKeysOrValuesForTenant("t-1", SuggestType.TAGK)).thenReturn(Mono.just(tagKeys));

    StepVerifier.create(suggestApiService.suggestTagKeys("t-1", "o", 10))
        .assertNext(tagKeysList -> {
          Assertions.assertThat(tagKeysList.size()).isEqualTo(2);
          Assertions.assertThat(tagKeysList).isEqualTo(List.of("os", "osx"));
        }).verifyComplete();

    //Test with the max limit
    StepVerifier.create(suggestApiService.suggestTagKeys("t-1", "o", 1))
        .assertNext(tagKeysList -> {
          Assertions.assertThat(tagKeysList.size()).isEqualTo(1);
        }).verifyComplete();

    //Test when metric doesn't match with any tag keys
    StepVerifier.create(suggestApiService.suggestTagKeys("t-1", "invalid", 10))
        .assertNext(tagKeysList -> {
          Assertions.assertThat(tagKeysList).isEmpty();
        }).verifyComplete();
  }

  @Test
  public void testSuggestTagValues() {

    List<String> tagValues1 = List.of("windows" , "linux", "linA", "linB");

    when(metadataService.getTagKeysOrValuesForTenant("t-1", SuggestType.TAGV)).thenReturn(Mono.just(tagValues1));

    StepVerifier.create(suggestApiService.suggestTagValues("t-1", "lin", 10))
        .assertNext(tagValuesList -> {
          Assertions.assertThat(tagValuesList.size()).isEqualTo(3);
          Assertions.assertThat(tagValuesList).isEqualTo(List.of("linux", "linA", "linB"));
        }).verifyComplete();

    //Test with the max limit
    StepVerifier.create(suggestApiService.suggestTagValues("t-1", "lin", 2))
        .assertNext(tagKeysList -> {
          Assertions.assertThat(tagKeysList.size()).isEqualTo(2);
        }).verifyComplete();

    //Test when metric doesn't match with any tag values
    StepVerifier.create(suggestApiService.suggestTagValues("t-1", "invalid", 10))
        .assertNext(tagKeysList -> {
          Assertions.assertThat(tagKeysList).isEmpty();
        }).verifyComplete();
  }

}
