package com.rackspace.ceres.app.config.configValidator;

import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import java.util.List;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.springframework.beans.BeanWrapperImpl;

public class ConcreteGranularityValidator implements ConstraintValidator<GranularityValidator, Object> {

  public void initialize() {

  }

  @Override
  public boolean isValid(Object o, ConstraintValidatorContext constraintValidatorContext) {
    List<Granularity> fieldValue = (List<Granularity>) new BeanWrapperImpl(o)
        .getPropertyValue("granularities");
    if(fieldValue == null) {
      return true;
    }

    fieldValue.sort((g1, g2) -> g1.getWidth().compareTo(g2.getWidth()));

    for(int i = 0; i < fieldValue.size(); i++) {
      if(i != fieldValue.size()-1 && fieldValue.get(i+1).getWidth().getSeconds() % fieldValue.get(i).getWidth().getSeconds() != 0) {
        return false;
      }
    }

    Granularity largest = null;
    for(Granularity granularity : fieldValue) {
      if(largest == null || granularity.getWidth().getSeconds() > largest.getWidth().getSeconds()) {
        largest = granularity;
      }
    }

    for(Granularity granularity : fieldValue) {
      if(largest.getWidth().getSeconds() % granularity.getWidth().getSeconds() != 0) {
        return false;
      }
    }

    return true;
  }
}
