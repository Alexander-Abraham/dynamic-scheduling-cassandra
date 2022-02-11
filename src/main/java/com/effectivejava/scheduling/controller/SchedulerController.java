package com.effectivejava.scheduling.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.effectivejava.scheduling.controller.model.QueryModel;
import com.effectivejava.scheduling.service.SchedulerService;

@RestController
public class SchedulerController {

	@Autowired
	private SchedulerService schedulerService;
	
		
	@RequestMapping(value = "/insertQueryData", method = RequestMethod.GET)
	public String insertQueryData() {
		schedulerService.insertQueryData();
		return "Data inserted successfully";
	}
	
	@RequestMapping(value = "/updateQueryData", method = RequestMethod.POST)
	public String updateQueryData(@RequestBody QueryModel queryModel) {
		schedulerService.updateQueryData(queryModel);
		return "Data updated successfully";
	}
	
	@RequestMapping(value = "/addNewQueryData", method = RequestMethod.POST)
	public String addNewQueryData(@RequestBody QueryModel queryModel) {
		schedulerService.addNewQueryData(queryModel);
		return "Data Saved successfully";
	}
	
	@RequestMapping(value = "/deleteQueryData", method = RequestMethod.POST)
	public void deleteQueryData(@RequestBody QueryModel queryModel) {
		schedulerService.deleteQueryData(queryModel);
	}
	
}
