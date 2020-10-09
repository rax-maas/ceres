package me.itzg.ceres;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.scheduler.Schedulers;

@SpringBootApplication
public class TsdbCassandraApplication {

	public static void main(String[] args) {
		Schedulers.enableMetrics();
		SpringApplication.run(TsdbCassandraApplication.class, args);
	}

}
