package cn.cassandra.quickstart;

import cn.cassandra.utils.CassandraUtils;
import com.datastax.driver.core.Session;

/**
 * Cassandra与java的连接应用测试
 */
public class CassandraKeySpace {

    public void createKeySpace() {
        Session session = null;
        try {
            session = CassandraUtils.getSession();

            //step3:执行查询
            String cqlsh = "CREATE KEYSPACE tp WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};";
            session.execute(cqlsh);

            cqlsh = "USE tp;";
            System.out.println("Create keyspace successfully");

        }finally {
            CassandraUtils.close();
        }
    }
    public static void dropKeySpace() {
        Session session = null;
        try {
            session = CassandraUtils.getSession();
            //step3:执行查询
            String cqlsh = "DROP KEYSPACE mytest;";
            session.execute(cqlsh);
        }finally {
            CassandraUtils.close();
        }
    }

    public static void main(String[] args) {
        CassandraKeySpace cassandraKeySpace = new CassandraKeySpace();
        cassandraKeySpace.createKeySpace();
//        cassandraKeySpace.dropKeySpace();
    }
}
