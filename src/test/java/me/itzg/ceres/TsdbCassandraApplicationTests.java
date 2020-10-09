package me.itzg.ceres;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class TsdbCassandraApplicationTests {
	@Container
	public static CassandraContainer<?> cassandraContainer = new CassandraContainer<>();
	@TestConfiguration
	@Import(CassandraContainerSetup.class)
	public static class TestConfig {
		@Bean
		CassandraContainer<?> cassandraContainer() {
			return cassandraContainer;
		}
	}

	@Test
	void contextLoads() {
	}

}
