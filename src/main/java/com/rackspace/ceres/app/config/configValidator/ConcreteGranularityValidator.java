package com.rackspace.ceres.app.config.configValidator;

import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import java.util.List;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.springframework.beans.BeanWrapperImpl;

public class ConcreteGranularityValidator implements ConstraintValidator<GranularityValidator, Object> {

  public void initialize() { }

  @Override
  public boolean isValid(Object o, ConstraintValidatorContext constraintValidatorContext) {
    List<Granularity> granularityList = (List<Granularity>) new BeanWrapperImpl(o)
        .getPropertyValue("granularities");
    if(granularityList == null) {
      return true;
    }

    granularityList.sort((g1, g2) -> g1.getWidth().compareTo(g2.getWidth()));

    for(int i = 0; i < granularityList.size()-1; i++) {
      if(i != granularityList.size() && granularityList.get(i+1).getWidth().getSeconds() % granularityList.get(i).getWidth().getSeconds() != 0) {
        return false;
      }
    }

    Granularity largest = null;
    for(Granularity granularity : granularityList) {
      if(largest == null || largest.getWidth().getSeconds() < granularity.getWidth().getSeconds()){
        largest = granularity;
      }
    }

    for(Granularity granularity : granularityList) {
      if(largest.getWidth().getSeconds() % granularity.getWidth().getSeconds() != 0) {
        return false;
      }
    }

    return true;
  }
}
