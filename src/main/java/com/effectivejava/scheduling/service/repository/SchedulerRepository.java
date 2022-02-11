package com.effectivejava.scheduling.service.repository;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.effectivejava.scheduling.service.entity.QueryEntity;

@Repository
public interface SchedulerRepository extends CassandraRepository<QueryEntity, Integer>{

	@Query("UPDATE QUERY_ENTITY SET QUERY=:query, FREQUENCY=:frequency WHERE ID =:id")
	public void updateById(@Param("id") int id, @Param("query") String query, @Param("frequency") int frequency);
}
