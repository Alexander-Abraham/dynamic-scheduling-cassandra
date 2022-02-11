package com.effectivejava.scheduling.service.dao;

import java.util.List;
import java.util.Optional;

import com.effectivejava.scheduling.service.entity.QueryEntity;

public interface SchedulerServiceDao {

	public List<QueryEntity> insertQueryData(List<QueryEntity> queryEntities);

	public void updateQueryData(QueryEntity queryEntity);

	public List<QueryEntity> getQueryData();

	public Optional<QueryEntity> findById(int id);
	
	public void addNewQueryData(QueryEntity queryEntity);

	public void deleteQueryData(QueryEntity queryEntity);

}
