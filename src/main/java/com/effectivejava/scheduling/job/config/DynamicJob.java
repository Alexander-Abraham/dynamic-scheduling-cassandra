package com.effectivejava.scheduling.job.config;

import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Component;

import com.effectivejava.scheduling.controller.model.QueryModel;
import com.effectivejava.scheduling.job.QueryJob;

@Component
public class DynamicJob {

	 @Autowired
	 private SchedulerFactoryBean schedulerFactoryBean;
	
	
	
	public void createSchedule(QueryModel queryModel) {
		
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put("JOB"+"_"+String.valueOf(queryModel.getId()), queryModel);
		JobKey jobKey = new JobKey("JOB"+"_"+String.valueOf(queryModel.getId()), "GRP1");
		JobDetail job = JobBuilder.newJob(QueryJob.class).withIdentity(jobKey).setJobData(jobDataMap).build();
		Trigger trigger = createTrigger(queryModel);
		try {
			Scheduler scheduler = schedulerFactoryBean.getScheduler();
			scheduler.start();
			scheduler.scheduleJob(job,trigger);
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
		
		//Scheduler scheduler= new StdSchedulerFactory("application.yml").getScheduler();
		
		
	}

	

	private Trigger createTrigger(QueryModel queryModel) {
		return TriggerBuilder.newTrigger().withIdentity("TRIGGER"+"_"+String.valueOf(queryModel.getId()),"GRP1")
				.withSchedule(SimpleScheduleBuilder.repeatSecondlyForever((int)(queryModel.getFrequency()/1000))).build();
	}
	
}
