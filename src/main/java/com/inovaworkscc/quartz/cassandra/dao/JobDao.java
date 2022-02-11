package com.inovaworkscc.quartz.cassandra.dao;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.oss.driver.api.core.cql.Row;
import com.inovaworkscc.quartz.cassandra.JobConverter;
import static com.inovaworkscc.quartz.cassandra.JobConverter.*;
import com.inovaworkscc.quartz.cassandra.db.CassandraConnectionManager;
import com.inovaworkscc.quartz.cassandra.db.CassandraDatabaseException;
import static com.inovaworkscc.quartz.cassandra.Constants.JOB_DATA;
import static com.inovaworkscc.quartz.cassandra.Constants.JOB_DATA_PLAIN;
import com.inovaworkscc.quartz.cassandra.util.Keys;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.*;

import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_GROUP;
import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_NAME;
import com.inovaworkscc.quartz.cassandra.util.QueryHelper;
import com.inovaworkscc.quartz.cassandra.util.SerialUtils;
import java.io.IOException;
import org.quartz.JobDataMap;

public class JobDao implements GroupedDao{
    
    public static final String TABLE_NAME_JOBS = "main.jobs";

    public static final String JOBS_GET_ALL = CassandraConnectionManager.registerStatement ("JOBS_GET_ALL", 
            "SELECT * FROM " + TABLE_NAME_JOBS
    );
    
    public static final String JOBS_DELETE_ALL = CassandraConnectionManager.registerStatement ("JOBS_DELETE_ALL", 
            "TRUNCATE " + TABLE_NAME_JOBS
    );
    
    public static final String JOBS_COUNT_BY_KEY = CassandraConnectionManager.registerStatement("JOBS_COUNT_BY_KEY",
            "SELECT COUNT(*) FROM " + TABLE_NAME_JOBS + " WHERE "
                    + KEY_NAME + " = ? AND "
                    + KEY_GROUP + " = ? AND "
    );
    
    public static final String JOBS_COUNT = CassandraConnectionManager.registerStatement("JOBS_COUNT",
            "SELECT COUNT(*) FROM " + TABLE_NAME_JOBS
    );
    
    public static final String JOBS_GET_BY_KEY_GROUP = CassandraConnectionManager.registerStatement("JOBS_GET_BY_KEY_GROUP",
            "SELECT * FROM " + TABLE_NAME_JOBS + " WHERE "
                    + KEY_GROUP + " IN ?"
    );
    
    public static final String JOBS_GET_BY_KEY = CassandraConnectionManager.registerStatement("JOBS_GET_BY_KEY",
            "SELECT * FROM " + TABLE_NAME_JOBS + " WHERE "
                    + KEY_NAME + " = ? AND "
                    + KEY_GROUP + " = ?"
    );
    
    public static final String JOBS_GET_BY_JOB_ID = CassandraConnectionManager.registerStatement("JOBS_GET_BY_JOB_ID",
            "SELECT * FROM " + TABLE_NAME_JOBS + " WHERE "
                    + JOB_ID + " = ?"+" ALLOW FILTERING"
    );
    
    public static final String JOBS_GET_DISTINCT_KEY_GROUP = CassandraConnectionManager.registerStatement("JOBS_GET_DISTINCT_KEY_GROUP",
            "SELECT DISTINCT " + KEY_GROUP + " FROM " + TABLE_NAME_JOBS
    );
    
    public static final String JOBS_GET_BY_KEY_GROUP_LIKE = CassandraConnectionManager.registerStatement("JOBS_GET_BY_KEY_GROUP_LIKE",
            "SELECT * FROM " + TABLE_NAME_JOBS + " WHERE "
                    + KEY_GROUP + "_index LIKE ?"
    );
        
    public static final String JOBS_DELETE_BY_KEY = CassandraConnectionManager.registerStatement("JOBS_DELETE_BY_KEY",
            "DELETE FROM " + TABLE_NAME_JOBS + " WHERE "
            + KEY_NAME + " = ? AND "
            + KEY_GROUP + " = ?"
    );

    public static final String JOBS_INSERT_JOB_DATA = CassandraConnectionManager.registerStatement("JOBS_INSERT_JOB_DATA",
            "INSERT INTO " + TABLE_NAME_JOBS + " (" + KEY_NAME + "," + KEY_GROUP + "," + KEY_GROUP + "_index" + "," + JOB_ID + "," + JOB_DESCRIPTION + "," + JOB_CLASS + "," + JOB_DURABILITY + "," + JOB_REQUESTS_RECOVERY + "," + JOB_DATA + ")" + "VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?)"
    );
    
    public static final String JOBS_INSERT_JOB_DATA_PLAIN = CassandraConnectionManager.registerStatement("JOBS_INSERT_JOB_DATA_PLAIN",
            "INSERT INTO " + TABLE_NAME_JOBS + " (" + KEY_NAME + "," + KEY_GROUP + "," + KEY_GROUP + "_index" + "," + JOB_ID + "," + JOB_DESCRIPTION + "," + JOB_CLASS + "," + JOB_DURABILITY + "," + JOB_REQUESTS_RECOVERY + "," + JOB_DATA_PLAIN + ")" + "VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?)"
    );
    
    public static final String JOBS_INSERT_NO_JOB_DATA_PLAIN = CassandraConnectionManager.registerStatement("JOBS_INSERT_NO_JOB_DATA_PLAIN",
            "INSERT INTO " + TABLE_NAME_JOBS + " (" + KEY_NAME + "," + KEY_GROUP + "," + KEY_GROUP + "_index" + "," + JOB_ID + "," + JOB_DESCRIPTION + "," + JOB_CLASS + "," + JOB_DURABILITY + "," + JOB_REQUESTS_RECOVERY + ")" + "VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?)"
    );
    
    private final QueryHelper queryHelper;
    private final JobConverter jobConverter;

    public JobDao(QueryHelper queryHelper, JobConverter jobConverter) {
        this.queryHelper = queryHelper;
        this.jobConverter = jobConverter;
    }

    public List<Row> clear() {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(JOBS_DELETE_ALL))
				.bind();
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        return rs.all();
    }

    public boolean exists(JobKey jobKey) {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(JOBS_COUNT_BY_KEY))
				.bind(jobKey.getName(), jobKey.getGroup());
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        Row r = rs.one();
        return r.getLong("count") > 0;
    }

    public Row getById(String id) {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(JOBS_GET_BY_JOB_ID))
				.bind(id);
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        Row r = rs.one();
        return r;
    }
    
    public Row getJob(JobKey jobKey) {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(JOBS_GET_BY_KEY))
				.bind(jobKey.getName(), jobKey.getGroup());
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        Row r = rs.one();
        return r;
    }

    public long getCount() {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(JOBS_COUNT))
				.bind();
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        Row r = rs.one();
        return r.getLong("count");
    }

    public List<String> getGroupNames() {

        List<String> ret = new ArrayList<>();
        BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(JOBS_GET_DISTINCT_KEY_GROUP))
				.bind();
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
      
        rs.forEach(row -> {
            ret.add(row.getString(KEY_GROUP));
        });
        
        return ret;
    }

    public List<Row> getJobs(GroupMatcher<JobKey> matcher) {
        
        String value = queryHelper.matchingKeysConditionForCassandra(matcher);
        BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(JOBS_GET_BY_KEY_GROUP_LIKE))
				.bind(value);
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        return rs.all();
    }
    
    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) {
        
        String value = queryHelper.matchingKeysConditionForCassandra(matcher);
        BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(JOBS_GET_BY_KEY_GROUP_LIKE))
				.bind(value);
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        Set<JobKey> keys = new HashSet<>();
        rs.forEach(row -> {
            keys.add(Keys.toJobKey(row));
        });
                
        return keys;
    }

    public Collection<String> idsOfMatching(GroupMatcher<JobKey> matcher) {
        
        List<Row> rows = getJobs(matcher);
        
        List<String> ret = new ArrayList<>();
        rows.forEach((row) -> {
            ret.add(row.getString(JOB_ID));
        });
        return ret;
    }

    public void remove(JobKey jobKey) {
    	  BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
  				.prepare(CassandraConnectionManager.getMapValue(JOBS_DELETE_BY_KEY))
  				.bind(jobKey.getName(), jobKey.getGroup());
          CassandraConnectionManager.getInstance().getSession().execute(bind);
       
    }

    public boolean requestsRecovery(JobKey jobKey) {
        //TODO check if it's the same as getJobDataMap?
        Row row = getJob(jobKey);
        
        return row.getBool(JobConverter.JOB_REQUESTS_RECOVERY);
    }

    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        Row row = getJob(jobKey);
        if (row == null) {
            //Return null if job does not exist, per interface
            return null;
        }
        return jobConverter.toJobDetail(row);
    }

    public String storeJobInCassandra(JobDetail newJob, boolean replaceExisting) throws JobPersistenceException {
        JobKey key = newJob.getKey();

        Row existingJob = getJob(key);

        JobDataMap jobDataMap = newJob.getJobDataMap();

        String jobId;
        if (existingJob != null && replaceExisting) {
            
            jobId = existingJob.getString(JOB_ID);
            
            storeJob(jobDataMap, key, jobId, newJob); //keyName, keyGroup, keyGroup_index, jobId, jobDescription, jobClass, durability, requestsRecovery, jobData
            //keyName, keyGroup, keyGroup_index, jobId, jobDescription, jobClass, durability, requestsRecovery, jobData

        } else if (existingJob == null) {
            
            jobId = UUID.randomUUID().toString();
            
            storeJob(jobDataMap, key, jobId, newJob);
        } else {
            jobId = existingJob.getString(JOB_ID);
        }

        return jobId;
    }

    private void storeJob(JobDataMap jobDataMap, JobKey key, String jobId, JobDetail newJob) throws JobPersistenceException, CassandraDatabaseException {
        
        BoundStatement bind;
        
        if(jobDataMap.isEmpty()){
        	 bind = CassandraConnectionManager.getInstance().getSession()
     				.prepare(CassandraConnectionManager.getMapValue(JOBS_INSERT_NO_JOB_DATA_PLAIN))
     				.bind(
     	                    key.getName(),
     	                    key.getGroup(),
     	                    key.getGroup(),
     	                    jobId,
     	                    newJob.getDescription(),
     	                    newJob.getJobClass().getName(),
     	                    newJob.isDurable(),
     	                    newJob.requestsRecovery()
     	            );
            // ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
           
        }
        if (jobConverter.jobDataConverter.isBase64Preferred()){
            
            String jobDataString;
            try {
                jobDataString = SerialUtils.serialize(jobDataMap);
            } catch (IOException e) {
                throw new JobPersistenceException("Could not serialise job data.", e);
            }
            bind = CassandraConnectionManager.getInstance().getSession()
     				.prepare(CassandraConnectionManager.getMapValue(JOBS_INSERT_JOB_DATA))
     				.bind(
     	                    key.getName(),
     	                    key.getGroup(),
     	                    key.getGroup(),
     	                    jobId,
     	                    newJob.getDescription(),
     	                    newJob.getJobClass().getName(),
     	                    newJob.isDurable(),
     	                    newJob.requestsRecovery(),
     	                    jobDataString
     	            );

        }else{
        	bind = CassandraConnectionManager.getInstance().getSession()
     				.prepare(CassandraConnectionManager.getMapValue(JOBS_INSERT_JOB_DATA_PLAIN))
     				.bind(
     	                    key.getName(),
     	                    key.getGroup(),
     	                    key.getGroup(),
     	                    jobId,
     	                    newJob.getDescription(),
     	                    newJob.getJobClass().getName(),
     	                    newJob.isDurable(),
     	                    newJob.requestsRecovery(),
     	                    jobDataMap.getWrappedMap()
     	            );
        	

        }
        
        CassandraConnectionManager.getInstance().getSession().execute(bind);
    }

    @Override
    public Set<String> groupsLike(String value) {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(JOBS_GET_BY_KEY_GROUP_LIKE))
				.bind(value);
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        Set<String> groups = new HashSet<>();
        rs.forEach(row -> {
            groups.add(row.getString(KEY_GROUP));
        });
        return groups;
    }

    @Override
    public List<Row> rowsInGroups(Set<String> groups) {

    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(JOBS_GET_BY_KEY_GROUP))
				.bind(groups);
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        return rs.all();
    }

    @Override
    public Set<String> allGroups() {

        return new HashSet<>(getGroupNames()); 
    }
}
