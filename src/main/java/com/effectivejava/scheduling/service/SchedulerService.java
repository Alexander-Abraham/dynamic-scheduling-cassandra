package com.effectivejava.scheduling.service;

import java.util.List;

import com.effectivejava.scheduling.controller.model.QueryModel;

public interface SchedulerService {

	public void insertQueryData();

	public void updateQueryData(QueryModel queryModel);

	public List<QueryModel> getQueryData();
	
	public QueryModel findbyId(int id);

	public void addNewQueryData(QueryModel queryModel);

	public void deleteQueryData(QueryModel queryModel);
	
	
	
}
