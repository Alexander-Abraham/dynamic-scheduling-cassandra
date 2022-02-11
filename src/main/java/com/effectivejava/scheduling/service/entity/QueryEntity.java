package com.effectivejava.scheduling.service.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.Table;

@Table(value="query_entity")
public class QueryEntity {

	@Id
	private int id;
	
	private String query;
	
	private int frequency;

	public QueryEntity() {
		
	}
	
	public QueryEntity(int id, String query, int frequency) {
		super();
		this.id = id;
		this.query = query;
		this.frequency = frequency;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

	@Override
	public String toString() {
		return "QueryEntity [id=" + id + ", query=" + query + ", frequency=" + frequency + "]";
	}
	
	
}
