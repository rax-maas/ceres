package com.rackspace.ceres.app.config.configValidator;

import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import java.util.List;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class ConcreteGranularityValidator implements ConstraintValidator<GranularityValidator, List<Granularity>> {

  public void initialize() { }

  @Override
  public boolean isValid(List<Granularity> granularityList, ConstraintValidatorContext constraintValidatorContext) {
    if(granularityList == null) {
      return true;
    }

    granularityList.sort((g1, g2) -> g1.getWidth().compareTo(g2.getWidth()));

    for(int i = 0; i < granularityList.size()-1; i++) {
      if(granularityList.get(i+1).getWidth().getSeconds() %
          granularityList.get(i).getWidth().getSeconds() != 0) {
        return false;
      }
    }
    return true;
  }
}
