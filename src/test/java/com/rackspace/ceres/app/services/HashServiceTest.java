package com.rackspace.ceres.app.services;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = { HashService.class })
@ActiveProfiles("test")
public class HashServiceTest {
    @Autowired
    HashService hashService;

    @Test
    void getHashCodeTest() {
        // TODO
    }

    @Test
    void getPartition() {
        // TODO
    }
}
