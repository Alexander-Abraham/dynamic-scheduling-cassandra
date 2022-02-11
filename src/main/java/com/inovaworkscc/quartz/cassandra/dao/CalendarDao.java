package com.inovaworkscc.quartz.cassandra.dao;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.oss.driver.api.core.cql.Row;
import org.quartz.Calendar;
import org.quartz.JobPersistenceException;

import com.inovaworkscc.quartz.cassandra.db.CassandraConnectionManager;
import com.inovaworkscc.quartz.cassandra.util.SerialUtils;
import com.inovaworkscc.quartz.cassandra.util.Keys.LockType;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import java.util.List;

public class CalendarDao {

    public static final String TABLE_NAME_CALENDARS = "main.calendars";

    static final String CALENDAR_NAME = "name";
    static final String CALENDAR_SERIALIZED_OBJECT = "serializedObject";
    
    public static final String CALENDARS_GET_ALL = CassandraConnectionManager.registerStatement ("CALENDARS_GET_ALL", 
             "SELECT * FROM " + TABLE_NAME_CALENDARS
    );
    
    public static final String CALENDARS_GET_ALL_NAMES = CassandraConnectionManager.registerStatement ("CALENDARS_GET_ALL_NAMES", 
             "SELECT " + CALENDAR_NAME + " FROM " + TABLE_NAME_CALENDARS
    );
    
    public static final String CALENDARS_GET = CassandraConnectionManager.registerStatement("CALENDARS_GET",
            "SELECT * FROM " + TABLE_NAME_CALENDARS + " WHERE "
            + CALENDAR_NAME + " = ?"
    );
    
    public static final String CALENDARS_INSERT = CassandraConnectionManager.registerStatement("CALENDARS_INSERT",
            "INSERT INTO " + TABLE_NAME_CALENDARS + " (" + CALENDAR_NAME + ", " + CALENDAR_SERIALIZED_OBJECT + ") VALUES ("
            + "?, "
            + "?)"
    );
    
    public static final String CALENDARS_DELETE_ALL = CassandraConnectionManager.registerStatement ("CALENDARS_DELETE_ALL", 
            "TRUNCATE " + TABLE_NAME_CALENDARS
    );
    
    public static final String CALENDARS_DELETE = CassandraConnectionManager.registerStatement("CALENDARS_DELETE",
            "DELETE FROM " + TABLE_NAME_CALENDARS + " WHERE "
            + CALENDAR_NAME + " = ?"
    );
    
    public static final String CALENDARS_COUNT = CassandraConnectionManager.registerStatement("CALENDARS_COUNT",
            "SELECT COUNT(*) FROM " + TABLE_NAME_CALENDARS
    );
    
    public CalendarDao() {}

    public void clear() {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(CALENDARS_DELETE_ALL))
				.bind();
		CassandraConnectionManager.getInstance().getSession().execute(bind);
    }

	public long getCount() {
		BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(CALENDARS_COUNT))
				.bind();
		ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
		Row r = rs.one();
		return r.getLong("count");
	}

    public boolean remove(String name) {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(CALENDARS_DELETE))
				.bind(name);
		ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
        return rs.one() != null;
    }

    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
        
        if(calName == null)
            return null;
        
//        try {
        BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(CALENDARS_GET))
				.bind(calName);
		ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);


//            ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement);
        Calendar calendar;
            
//            Row r = rs.get().one();
        Row r = rs.one();

        if (r != null){

            ByteBuffer bb = r.getByteBuffer("serializedObject");

            calendar = SerialUtils.deserialize(bb.array(), Calendar.class);

            return calendar;
        }
//        } catch (InterruptedException ex) {
//            throw new JobPersistenceException(ex.getMessage(), ex);
//        } catch (ExecutionException ex) {
//            throw new JobPersistenceException(ex.getMessage(), ex);
//        }
        
        
        return null;
    }

    public void store(String name, Calendar calendar) throws JobPersistenceException {
    	BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(CALENDARS_GET))
				.bind(name, ByteBuffer.wrap(SerialUtils.serialize(calendar)));
		CassandraConnectionManager.getInstance().getSession().execute(bind);
    }

    public List<String> retrieveCalendarNames() {
        
        List<String> ret = new ArrayList<>();
        BoundStatement bind = CassandraConnectionManager.getInstance().getSession()
				.prepare(CassandraConnectionManager.getMapValue(CALENDARS_GET_ALL_NAMES))
				.bind();
		ResultSet rs = CassandraConnectionManager.getInstance().getSession().execute(bind);
		
        rs.forEach(row -> {
            ret.add(row.getString("name"));
        });
                
        return ret;
    }
}
