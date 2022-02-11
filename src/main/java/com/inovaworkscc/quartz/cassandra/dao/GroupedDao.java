package com.inovaworkscc.quartz.cassandra.dao;

import com.datastax.oss.driver.api.core.cql.Row;
import java.util.List;
import java.util.Set;

/**
 *
 * @author andreassilva
 */
public interface GroupedDao {
    
    public Set<String> groupsLike(String filter);
    public List<Row> rowsInGroups(Set<String> groups);
    public Set<String> allGroups();
}
