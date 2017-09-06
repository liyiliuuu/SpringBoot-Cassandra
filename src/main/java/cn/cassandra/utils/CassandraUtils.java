package cn.cassandra.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * @Author: Amos
 * @Description: 这个类主要获取Cassandra的一些对象，如session等
 * @Date: 9/6/2017 4:24 PM
 */
public class CassandraUtils {

    //step1:创建一个cluster集群对象
    private static Cluster cluster = null;
    /**
     * @Author: Amos
     * @Description: 主要用来获取操作cassandra的session对象
     * @Param: keyspace 需要连接的keyspace空间，可以为空
     * @Return: Session
     * @Date: 9/6/2017 4:26 PM
     *
     */
    public static Session getSession(String keyspace) {
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        //step2:创建会话对象
        Session session = cluster.connect(keyspace); //这里可以指定已经存在的表空间
        return session;
    }

    /**
     * @Author: Amos
     * @Description: 主要用来获取操作cassandra的session对象
     * @Return: Session
     * @Date: 9/6/2017 4:26 PM
     *
     */
    public static Session getSession() {
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        //step2:创建会话对象
        return cluster.connect();
    }

    public static void close() {
        if(cluster != null) {
            cluster.close();
        }
    }
}
