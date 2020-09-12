package me.itzg.tsdbcassandra.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class SeriesSetServiceTest {

  @Test
  void buildSeriesSet() {
    final SeriesSetService seriesSetService = new SeriesSetService();

    final String result = seriesSetService.buildSeriesSet("name_here", Map.of(
        "os", "linux",
        "host", "server-1",
        "deployment", "prod"
    ));

    assertThat(result).isEqualTo("name_here,deployment=prod,host=server-1,os=linux");
  }
}
