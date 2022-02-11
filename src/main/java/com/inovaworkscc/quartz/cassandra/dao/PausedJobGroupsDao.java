package com.inovaworkscc.quartz.cassandra.dao;

import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_GROUP;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.inovaworkscc.quartz.cassandra.db.CassandraConnectionManager;

public class PausedJobGroupsDao implements GroupedDao{

    public static final String TABLE_NAME_PAUSED_JOB_GROUPS = "main.paused_job_groups";
        
    public static final String PAUSED_JOB_GROUPS_GET_ALL = CassandraConnectionManager.registerStatement ("PAUSED_JOB_GROUPS_GET_ALL", 
             "SELECT * FROM " + TABLE_NAME_PAUSED_JOB_GROUPS
    );
    
    public static final String PAUSED_JOB_GROUPS_INSERT = CassandraConnectionManager.registerStatement("PAUSED_JOB_GROUPS_INSERT",
            "INSERT INTO " + TABLE_NAME_PAUSED_JOB_GROUPS + " (" + KEY_GROUP + ") VALUES ("
            + "?)"
    );
    
    public static final String PAUSED_JOB_GROUPS_DELETE_ALL = CassandraConnectionManager.registerStatement ("PAUSED_JOB_GROUPS_DELETE_ALL", 
            "TRUNCATE " + TABLE_NAME_PAUSED_JOB_GROUPS
    );
    
    public static final String PAUSED_JOB_GROUPS_DELETE_MANY = CassandraConnectionManager.registerStatement("PAUSED_JOB_GROUPS_DELETE",
            "DELETE FROM " + TABLE_NAME_PAUSED_JOB_GROUPS + " WHERE "
            + KEY_GROUP + " IN ?"
    );
    
    public static final String PAUSED_JOB_GROUPS_GET_DISTINCT_KEY_GROUP = CassandraConnectionManager.registerStatement("PAUSED_JOB_GROUPS_GET_DISTINCT_KEY_GROUP",
            "SELECT DISTINCT " + KEY_GROUP + " FROM " + TABLE_NAME_PAUSED_JOB_GROUPS
    );
    
    public static final String PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP = CassandraConnectionManager.registerStatement("PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP",
            "SELECT * FROM " + TABLE_NAME_PAUSED_JOB_GROUPS + " WHERE "
                    + KEY_GROUP + " IN ?"
    );
    
    public static final String PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP_LIKE = CassandraConnectionManager.registerStatement("PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP_LIKE",
            "SELECT * FROM " + TABLE_NAME_PAUSED_JOB_GROUPS + " WHERE "
                    + KEY_GROUP + "_index LIKE ?"
    );
    
    public PausedJobGroupsDao() {}

    public HashSet<String> getPausedGroups() {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(PAUSED_JOB_GROUPS_GET_ALL))
				.bind();
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        HashSet<String> ret =  new HashSet<>();
         rs.forEach(row -> {
            ret.add(row.getString(KEY_GROUP));
        });
        
        return ret;
    }

    public void pauseGroups(List<String> groups) {
        if (groups == null) {
            throw new IllegalArgumentException("groups cannot be null!");
        }
        
        BatchStatement batchStatement = BatchStatement.newInstance(DefaultBatchType.UNLOGGED);
        
        groups.stream().map((s) -> {
        	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
    				.prepare(CassandraConnectionManager.getMapValue(PAUSED_JOB_GROUPS_INSERT))
    				.bind(s);
            
            return bind;
        }).forEachOrdered((boundStatement) -> {
            batchStatement.add(boundStatement);
        });
        
        CassandraConnectionManager.getInstance().getSession().execute(batchStatement);
    }

    public void remove() {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(PAUSED_JOB_GROUPS_DELETE_ALL))
				.bind();
        CassandraConnectionManager.getInstance().getSession().execute(bind);
       
    }

    public void unpauseGroups(Collection<String> groups) {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(PAUSED_JOB_GROUPS_DELETE_MANY))
				.bind(groups);
        CassandraConnectionManager.getInstance().getSession().execute(bind);
        
       
    }
    
    @Override
    public Set<String> groupsLike(String value) {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP_LIKE))
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
				.prepare(CassandraConnectionManager.getMapValue(PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP))
				.bind(groups);
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        return rs.all();
    }

    @Override
    public Set<String> allGroups() {

        Set<String> ret = new HashSet<>();
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(PAUSED_JOB_GROUPS_GET_DISTINCT_KEY_GROUP))
				.bind();
        ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        rs.forEach(row -> {
            ret.add(row.getString(KEY_GROUP));
        });
        
        return ret; 
    }
}
