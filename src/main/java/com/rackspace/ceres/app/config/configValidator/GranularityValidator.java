package com.rackspace.ceres.app.config.configValidator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.Payload;

@Constraint(validatedBy = ConcreteGranularityValidator.class)
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface GranularityValidator {
  String message() default "Granularities are not multiples of each other";
  Class<?>[] groups() default {};
  Class<? extends Payload>[] payload() default {};
}
