package com.effectivejava.scheduling.job.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

@Configuration
public class QuartzConfig {

	@Bean
	public SchedulerFactoryBean schedulerFactoryBean() {
		SchedulerFactoryBean scheduler = new SchedulerFactoryBean();
		scheduler.setApplicationContextSchedulerContextKey("applicationContext");
		scheduler.setConfigLocation(new ClassPathResource("application.yml"));
		scheduler.setWaitForJobsToCompleteOnShutdown(true);
		return scheduler;
	}
}
