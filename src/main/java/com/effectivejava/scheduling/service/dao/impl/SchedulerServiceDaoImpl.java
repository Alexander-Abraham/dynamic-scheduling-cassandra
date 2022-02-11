package com.effectivejava.scheduling.service.dao.impl;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.effectivejava.scheduling.service.dao.SchedulerServiceDao;
import com.effectivejava.scheduling.service.entity.QueryEntity;
import com.effectivejava.scheduling.service.repository.SchedulerRepository;

@Service
public class SchedulerServiceDaoImpl implements SchedulerServiceDao {

	@Autowired
	private SchedulerRepository schedulerRepository;
	
	@Override
	public List<QueryEntity> insertQueryData(List<QueryEntity> queryEntities) {
		List<QueryEntity> saveAll = schedulerRepository.saveAll(queryEntities);
		return saveAll;
	}

	@Override
	public void updateQueryData(QueryEntity queryEntity) {
		schedulerRepository.updateById(queryEntity.getId(), queryEntity.getQuery(), queryEntity.getFrequency());
	}

	@Override
	public List<QueryEntity> getQueryData() {
		return schedulerRepository.findAll();
	}

	@Override
	public Optional<QueryEntity> findById(int id) {
		return schedulerRepository.findById(id);
	}

	@Override
	public void addNewQueryData(QueryEntity queryEntity) {
		schedulerRepository.save(queryEntity);
		
	}

	@Override
	public void deleteQueryData(QueryEntity queryEntity) {
		schedulerRepository.deleteById(queryEntity.getId());
		
	}

}
