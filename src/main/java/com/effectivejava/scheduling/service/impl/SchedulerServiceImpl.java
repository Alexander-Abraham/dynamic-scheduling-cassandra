package com.effectivejava.scheduling.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Service;

import com.effectivejava.scheduling.controller.model.QueryModel;
import com.effectivejava.scheduling.job.config.DynamicJob;
import com.effectivejava.scheduling.service.SchedulerService;
import com.effectivejava.scheduling.service.dao.SchedulerServiceDao;
import com.effectivejava.scheduling.service.entity.QueryEntity;

@Service
public class SchedulerServiceImpl implements SchedulerService {
	
	@Autowired
	private SchedulerServiceDao schedulerServiceDao;
	
	@Autowired
	private DynamicJob dynamicJob;
	
	 @Autowired
	 private SchedulerFactoryBean schedulerFactoryBean;
	
	@Override
	public void insertQueryData() {
		
		List<QueryEntity> queryEntities = new ArrayList<QueryEntity>();
		queryEntities.add(new QueryEntity(1, "Scheduler Running every 5 sec", 5000));
		queryEntities.add(new QueryEntity(2, "Scheduler Running every 10 sec", 10000));
		queryEntities.add(new QueryEntity(3, "Scheduler Running every 15 sec", 15000));
		queryEntities.add(new QueryEntity(4, "Scheduler Running every 20 sec", 20000));
		queryEntities.add(new QueryEntity(5, "Scheduler Running every 25 sec", 25000));
		List<QueryEntity> insertQueryData = schedulerServiceDao.insertQueryData(queryEntities);
		List<QueryModel> queryModels = new ArrayList<>();
		
		for(QueryEntity queryEntity : insertQueryData) {
			QueryModel queryModel = new QueryModel();
			BeanUtils.copyProperties(queryEntity, queryModel);
			queryModels.add(queryModel);
		}
		for(QueryModel queryModel : queryModels) {
			dynamicJob.createSchedule(queryModel);
		}
	}

	@Override
	public void updateQueryData(QueryModel queryModel) {
		QueryEntity queryEntity = new QueryEntity();
		BeanUtils.copyProperties(queryModel, queryEntity);
		schedulerServiceDao.updateQueryData(queryEntity);
		updateScheduleJob(queryModel);
	}
	
	private void updateScheduleJob(QueryModel queryModel) {
		try {
			Scheduler scheduler = schedulerFactoryBean.getScheduler();
			JobKey jobKey = new JobKey(String.valueOf(queryModel.getId()));	
			scheduler.deleteJob(jobKey);
			dynamicJob.createSchedule(queryModel);
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<QueryModel> getQueryData() {
		List<QueryEntity> queryEntities = schedulerServiceDao.getQueryData();
		List<QueryModel> queryModels = new ArrayList<QueryModel>();
		
		for(QueryEntity queryEntity : queryEntities) {
			QueryModel queryModel = new QueryModel();
			BeanUtils.copyProperties(queryEntity, queryModel);
			queryModels.add(queryModel);
		}
		return queryModels;
	}

	@Override
	public QueryModel findbyId(int id) {
		Optional<QueryEntity> queryEntity = schedulerServiceDao.findById(id);
		QueryModel queryModel = new QueryModel();
		BeanUtils.copyProperties(queryEntity, queryModel);
		return queryModel;
	}

	@Override
	public void addNewQueryData(QueryModel queryModel) {
		QueryEntity queryEntity = new QueryEntity();
		BeanUtils.copyProperties(queryModel, queryEntity);
		if(schedulerServiceDao.findById(queryModel.getId()) != null) {
			updateScheduleJob(queryModel);
		}else {
			schedulerServiceDao.addNewQueryData(queryEntity);
			dynamicJob.createSchedule(queryModel);
		}
		
	}

	@Override
	public void deleteQueryData(QueryModel queryModel) {
		QueryEntity queryEntity = new QueryEntity();
		BeanUtils.copyProperties(queryModel, queryEntity);
		schedulerServiceDao.deleteQueryData(queryEntity);
		
		try {
			Scheduler scheduler = new StdSchedulerFactory().getScheduler();
			JobKey jobKey = new JobKey(String.valueOf(queryModel.getId()));	
			scheduler.deleteJob(jobKey);
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
		
	}
}
