package com.inovaworkscc.quartz.cassandra.dao;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.oss.driver.api.core.cql.Row;
import com.inovaworkscc.quartz.cassandra.cluster.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import com.inovaworkscc.quartz.cassandra.db.CassandraConnectionManager;
import com.inovaworkscc.quartz.cassandra.util.Clock;

public class SchedulerDao {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerDao.class);

    public static final String TABLE_NAME_SCHEDULERS = "main.paused_trigger_groups";
    
    public static final String SCHEDULER_NAME_FIELD = "schedulerName";
    public static final String INSTANCE_ID_FIELD = "instanceId";
    public static final String LAST_CHECKIN_TIME_FIELD = "lastCheckinTime";
    public static final String CHECKIN_INTERVAL_FIELD = "checkinInterval";
    
    public static final String SCHEDULERS_GET_ALL = CassandraConnectionManager.registerStatement ("SCHEDULERS_GET_ALL", 
            "SELECT * FROM " + TABLE_NAME_SCHEDULERS
    );
    
    public static final String SCHEDULERS_INSERT = CassandraConnectionManager.registerStatement("SCHEDULERS_INSERT",
            "INSERT INTO " + TABLE_NAME_SCHEDULERS + " (" + SCHEDULER_NAME_FIELD + "," + INSTANCE_ID_FIELD + "," + LAST_CHECKIN_TIME_FIELD + "," + CHECKIN_INTERVAL_FIELD + ")" + "VALUES ("
                + "?, "
                + "?, "
                + "?, "
                + "?)"
    );
    
    public static final String SCHEDULERS_UPSERT = CassandraConnectionManager.registerStatement("SCHEDULERS_UPSERT",
            "UPDATE " + TABLE_NAME_SCHEDULERS + " SET " 
                + LAST_CHECKIN_TIME_FIELD + " = ? ,"
                + CHECKIN_INTERVAL_FIELD + " = ? "
                + "WHERE "
                + SCHEDULER_NAME_FIELD + " = ? AND "
                + INSTANCE_ID_FIELD + " = ?"
    );
    
     public static final String SCHEDULERS_GET_BY_NAME_INSTANCE_ID = CassandraConnectionManager.registerStatement("SCHEDULERS_GET_BY_NAME_INSTANCE_ID",
            "SELECT * FROM " + TABLE_NAME_SCHEDULERS + " WHERE "
                    + SCHEDULER_NAME_FIELD + " = ? AND "
                    + INSTANCE_ID_FIELD + " = ?"
    );
    
    public static final String SCHEDULERS_DELETE = CassandraConnectionManager.registerStatement("SCHEDULERS_DELETE",
            "DELETE FROM " + TABLE_NAME_SCHEDULERS + " WHERE "
            + SCHEDULER_NAME_FIELD + " = ? AND "
            + INSTANCE_ID_FIELD + " = ?"
    );
     
    public final String schedulerName;
    public final String instanceId;
    public final long clusterCheckinIntervalMillis;
    public final Clock clock;

    public SchedulerDao(String schedulerName,
                        String instanceId, long clusterCheckinIntervalMillis, Clock clock) {
        this.schedulerName = schedulerName;
        this.instanceId = instanceId;
        this.clusterCheckinIntervalMillis = clusterCheckinIntervalMillis;
        this.clock = clock;
    }

    /**
     * Checks-in in cluster to inform other nodes that its alive.
     */
    public void checkIn() {
        long lastCheckinTime = clock.millis();

//        LOG.debug("Saving node data: name='{}', id='{}', checkin time={}, interval={}",
//                schedulerName, instanceId, lastCheckinTime, clusterCheckinIntervalMillis);

        // If not found Cassandra will create a new entry with content from filter and update.
        BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
  				.prepare(CassandraConnectionManager.getMapValue(SCHEDULERS_UPSERT)).bind(lastCheckinTime, clusterCheckinIntervalMillis, schedulerName, instanceId);
    	 CassandraConnectionManager.getInstance().getSession().execute(bind);
    }

    /**
     * @param instanceId
     * @return Scheduler or null when not found
     */
    public Scheduler findInstance(String instanceId) {
//        LOG.debug("Finding scheduler instance: {}", instanceId);
    	  BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
    				.prepare(CassandraConnectionManager.getMapValue(SCHEDULERS_GET_BY_NAME_INSTANCE_ID)).bind(schedulerName, instanceId);
      	 ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        
       
        
        Row row = rs.one();
                
        Scheduler scheduler = null;
        if (row != null) {
            scheduler = toScheduler(row);
//            LOG.debug("Returning scheduler instance '{}' with last checkin time: {}",
//                    scheduler.getInstanceId(), scheduler.getLastCheckinTime());
        } else {
//            LOG.info("Scheduler instance '{}' not found.");
        }
        return scheduler;
    }

    public boolean isNotSelf(Scheduler scheduler) {
        return !instanceId.equals(scheduler.getInstanceId());
    }

    /**
     * Return all scheduler instances in ascending order by last check-in time.
     *
     * @return scheduler instances ordered by last check-in time
     */
    public List<Scheduler> getAllByCheckinTime() {
        final List<Scheduler> schedulers = new LinkedList<>();
        
        BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(SCHEDULERS_GET_ALL)).bind();
  	 ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
  
        
        rs.forEach(row -> {
            schedulers.add(toScheduler(row));
        });
        
        //Sort CheckinTime Ascending
        schedulers.sort((Scheduler o1, Scheduler o2) -> Long.valueOf(o1.getLastCheckinTime()).compareTo(o2.getLastCheckinTime()));
        
        return schedulers;
    }

    /**
     * Remove selected scheduler instance entry from database.
     *
     * The scheduler is selected based on its name, instanceId, and lastCheckinTime.
     * If the last check-in time is different, then it is not removed, for it might
     * have gotten back to live.
     *
     * @param instanceId       instance id
     * @param lastCheckinTime  last time scheduler has checked in
     *
     * @return when removed successfully
     */
    public boolean remove(String instanceId, long lastCheckinTime) {
//        LOG.info("Removing scheduler: {},{},{}",
//                schedulerName, instanceId, lastCheckinTime);
        
        boolean ret = false;
        
        Scheduler foundInstance = findInstance(instanceId);
        if (foundInstance != null) {
            if(foundInstance.getCheckinInterval() == lastCheckinTime){
            	 BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
         				.prepare(CassandraConnectionManager.getMapValue(SCHEDULERS_DELETE)).bind(lastCheckinTime, clusterCheckinIntervalMillis, schedulerName, instanceId);
           	  CassandraConnectionManager.getInstance().getSession().execute(bind);
            }
        }

//        log.info("Result of removing scheduler ({},{},{}): {}",
//            schedulerName, instanceId, lastCheckinTime, ret);

        return ret;
    }

    private Scheduler toScheduler(Row row) {
        return new Scheduler(
                row.getString(SCHEDULER_NAME_FIELD),
                row.getString(INSTANCE_ID_FIELD),
                row.getLong(LAST_CHECKIN_TIME_FIELD),
                row.getLong(CHECKIN_INTERVAL_FIELD));
    }
}
