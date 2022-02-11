package com.effectivejava.scheduling.job;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.effectivejava.scheduling.controller.model.QueryModel;

public class QueryJob implements Job{

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		JobDetail jobDetail = context.getJobDetail();
		jobDetail.getJobDataMap().get(jobDetail.getKey().getName());
		System.out.println((QueryModel)jobDetail.getJobDataMap().get(jobDetail.getKey().getName()));
	}

}
