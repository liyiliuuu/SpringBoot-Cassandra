package cn.cassandra.quickstart;

import cn.cassandra.utils.CassandraUtils;
import com.datastax.driver.core.Session;

/**
 * @Author: Amos
 * @Description: 演示通过API 操作 Cassandra 表
 * @Date: 9/6/2017 4:36 PM
 */
public class CassandraTable {

    /**
     * @Author: Amos
     * @Description: 此方法用于演示创建表的API
     * @Date: 9/6/2017 4:37 PM
     *
     */
    public void createTable() {
        try {
            Session session = CassandraUtils.getSession("tp");
            String cqlsh = "CREATE TABLE emp ("
                    + "emp_id int PRIMARY KEY,"
                    + "emp_name text,"
                    + "emp_city text,"
                    + "emp_sal varint,"
                    + "emp_phone varint"
                    + ");";
            session.execute(cqlsh);
            System.out.println("Table created");
        }finally {
           CassandraUtils.close();
        }
    }


    public void updateTable() {
        /**
         * @Author: Amos
         * @Description: 此方法用于演示更新表的API
         * @Date: 9/6/2017 4:40 PM
         *
         */
        try {
            Session session = CassandraUtils.getSession("tp");
//            String cqlsh = "ALTER TABLE emp ADD emp_email text;";
//            session.execute(cqlsh);
//            System.out.println("ADD column successfully");

            String cqlsh = "ALTER TABLE emp DROP emp_phone;";
            session.execute(cqlsh);
            System.out.println("DROP column successfully");

        }finally {
            CassandraUtils.close();
        }
    }


    public void deleteTable() {
        /**
         * @Author: Amos
         * @Description: 此方法用于演示删除表的API
         * @Date: 9/6/2017 4:40 PM
         *
         */
        try {
            Session session = CassandraUtils.getSession("tp");
            String cqlsh = "DROP TABLE emp;";
            session.execute(cqlsh);
        }finally {
            CassandraUtils.close();
        }
    }


    public void truncateTable() {
        /**
         * @Author: Amos
         * @Description: 此方法用于演示截断表的API,截断表时，表的所有行都将永久删除
         * @Date: 9/6/2017 4:41 PM
         *
         */
        try {
            Session session = CassandraUtils.getSession("tp");
            String cqlsh = "TRUNCATE emp;";
            session.execute(cqlsh);
        }finally {
            CassandraUtils.close();
        }
    }

    public void createIndex() {
        /**
         * @Author: Amos
         * @Description: 此方法用于演示创建索引的API。
         * @Date: 9/6/2017 5:33 PM
         */
        try {
            Session session = CassandraUtils.getSession("tp");
            String cqlsh = "CREATE INDEX name ON emp(emp_name);";
            session.execute(cqlsh);
            System.out.println("Index created");
        }finally {
            CassandraUtils.close();
        }
    }

    /**
     * @Author: Amos
     * @Description: 此方法用于演示删除索引的API
     * @Date: 9/6/2017 5:21 PM
     *
     */
    public void deleteIndex() {
        try {
            Session session = CassandraUtils.getSession("tp");
            String cqlsh = "DROP INDEX name ;";
            session.execute(cqlsh);
            System.out.println("Index deleted");
        }finally {
            CassandraUtils.close();
        }
    }

    public void batchOperation() {
        try {
            Session session = CassandraUtils.getSession("tp");
            String cqlsh = " BEGIN BATCH INSERT INTO emp (emp_id, emp_city,emp_name, emp_phone, emp_sal) "
                    + "values( 4,'Pune','rajeev',9848022331, 30000);"
                    + "UPDATE emp SET emp_sal = 50000 WHERE emp_id =3;"
                    + "DELETE emp_city FROM emp WHERE emp_id = 2;"
                    + "APPLY BATCH;";
            session.execute(cqlsh);
            System.out.println("Index deleted");
        }finally {
            CassandraUtils.close();
        }
    }

    public static void main(String[] args) {
        CassandraTable cassandraTable = new CassandraTable();
//        cassandraTable.createTable();
//        cassandraTable.updateTable();
//        cassandraTable.deleteTable();
//        cassandraTable.createIndex();
//        cassandraTable.deleteIndex();
    }

}
