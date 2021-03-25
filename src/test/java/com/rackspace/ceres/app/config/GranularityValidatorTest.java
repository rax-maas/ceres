package com.rackspace.ceres.app.config;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.config.configValidator.ConcreteGranularityValidator;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;


public class GranularityValidatorTest {

  @Test
  public void invalidGranularities() {
    Granularity granularity = new Granularity();
    granularity.setWidth(Duration.parse("PT3M"));
    List<Granularity> granularities = new LinkedList();
    granularities.add(granularity);
    granularity = new Granularity();
    granularity.setWidth(Duration.parse("PT7M"));
    granularities.add(granularity);

    ConcreteGranularityValidator validator = new ConcreteGranularityValidator();
    assertThat(validator.isValid(granularities, null)).isFalse();
  }

  @Test
  public void validGranularities() {
    Granularity granularity = new Granularity();
    granularity.setWidth(Duration.parse("PT3M"));
    List<Granularity> granularities = new LinkedList();
    granularities.add(granularity);
    granularity = new Granularity();
    granularity.setWidth(Duration.parse("PT9M"));
    granularities.add(granularity);

    ConcreteGranularityValidator validator = new ConcreteGranularityValidator();
    assertThat(validator.isValid(granularities, null)).isTrue();
  }

  @Test
  public void oneGranularity() {
    Granularity granularity = new Granularity();
    granularity.setWidth(Duration.parse("PT3M"));
    List<Granularity> granularities = new LinkedList();
    granularities.add(granularity);

    ConcreteGranularityValidator validator = new ConcreteGranularityValidator();
    assertThat(validator.isValid(granularities, null)).isTrue();
  }

  @Test
  public void emptyListGranularity() {
    List<Granularity> granularities = new LinkedList();

    ConcreteGranularityValidator validator = new ConcreteGranularityValidator();
    assertThat(validator.isValid(granularities, null)).isTrue();
  }

  @Test
  public void nullGranularities() {
    ConcreteGranularityValidator validator = new ConcreteGranularityValidator();
    assertThat(validator.isValid(null, null)).isTrue();
  }
}
