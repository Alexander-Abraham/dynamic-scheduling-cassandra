package com.effectivejava.scheduling.job.config;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.effectivejava.scheduling.controller.model.QueryModel;
import com.effectivejava.scheduling.service.SchedulerService;


public class ScheduleConfiguration {

	@Autowired
	private SchedulerService schedulerService;
	
	@Autowired
	private DynamicJob dynamicJob;
	
	public void createDynamicSchedules() {
		System.out.println("Inside Scheduling configurations.....");
		List<QueryModel> queryModels = schedulerService.getQueryData();
		if(queryModels.isEmpty()) {
			System.out.println("SCHEDULES ARE EMPTY");
			return;
		}
		for(QueryModel queryModel : queryModels) {
			dynamicJob.createSchedule(queryModel);
		}
	}
}
