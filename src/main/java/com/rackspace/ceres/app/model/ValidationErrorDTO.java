package com.rackspace.ceres.app.model;

import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
public class ValidationErrorDTO {
    private List<FieldErrorDTO> fieldErrors = new ArrayList<>();
    public ValidationErrorDTO() {
    }

    public void addFieldError(String path, String message) {
        FieldErrorDTO error = new FieldErrorDTO(path, message);
        fieldErrors.add(error);
    }
}