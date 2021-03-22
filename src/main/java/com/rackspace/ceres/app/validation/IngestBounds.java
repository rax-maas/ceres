package com.rackspace.ceres.app.validation;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Constraint(validatedBy = IngestBoundsValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface IngestBounds {
    String message() default "Provided Ingest metric timestamps should be in between configured bounds.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
