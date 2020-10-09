package me.itzg.ceres.services;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import me.itzg.ceres.model.MetricNameAndTags;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = {SeriesSetService.class})
@ActiveProfiles("test")
class SeriesSetServiceTest {

  @Autowired
  SeriesSetService seriesSetService;

  @Test
  void buildSeriesSet() {
    final String result = seriesSetService.buildSeriesSet("name_here", Map.of(
        "os", "linux",
        "host", "server-1",
        "deployment", "prod"
    ));

    assertThat(result).isEqualTo("name_here,deployment=prod,host=server-1,os=linux");
  }

  @Test
  void expandSeriesSet() {
    final MetricNameAndTags result = seriesSetService
        .expandSeriesSet("name_here,deployment=prod,host=server-1,os=linux");

    assertThat(result).isNotNull();
    assertThat(result.getMetricName()).isEqualTo("name_here");
    assertThat(result.getTags()).containsOnly(
        Map.entry("deployment", "prod"),
        Map.entry("host", "server-1"),
        Map.entry("os", "linux")
    );
  }
}
