package com.rackspace.ceres.app.validation;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Constraint(validatedBy = InBetweenValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface InBetween {
    String message() default "Ingested metric timestamps should in between configured bounds";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
