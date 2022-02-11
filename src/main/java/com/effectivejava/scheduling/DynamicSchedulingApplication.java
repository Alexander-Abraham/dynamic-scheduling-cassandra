package com.effectivejava.scheduling;

import java.nio.file.Path;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.effectivejava.scheduling.connection.DataStaxAstraProperties;
import com.effectivejava.scheduling.job.config.ScheduleConfiguration;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class DynamicSchedulingApplication {

	public static void main(String[] args) {
		SpringApplication.run(DynamicSchedulingApplication.class, args);
	}
	
	@Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
	
	@Bean(initMethod = "createDynamicSchedules")
	public ScheduleConfiguration scheduleConfiguration() {
		return new ScheduleConfiguration();
		
	}

}
