package me.itzg.tsdbcassandra.config;

import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

/**
 * Parses a string containing comma separated list of integers or ranges of integers,
 * such as "1,5-8,12,15-18"
 */
@Component
@ConfigurationPropertiesBinding
public class IntegerToIntegerSetConverter implements Converter<Integer, IntegerSet> {

  @Override
  public IntegerSet convert(Integer input) {
    return new IntegerSet(Set.of(input));
  }
}
