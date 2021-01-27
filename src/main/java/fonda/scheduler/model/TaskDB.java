package fonda.scheduler.model;

import com.zaxxer.hikari.HikariDataSource;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

class TaskDB {


    private static HikariDataSource ds = new HikariDataSource();

    private TaskDB() {

    }

    static {
        ds.setJdbcUrl("jdbc:postgresql://<ip>:5432/<dbName>");
        ds.setUsername("<name>");
        ds.setPassword("<pw>");
        ds.setDriverClassName("org.postgresql.Driver");
    }

    public static DataSource getDataSource() {
        return ds;
    }

    public static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    public static void addSchedulingReportToDB(Pod pod, Node node, Integer score) {


        try {
            Connection connection = getConnection();
            Statement statement = connection.createStatement();

            System.out.println("INSERT INTO ScheduleReport (pod_name, node_name, score) VALUES (" + pod.getMetadata().getName() + ", " + node.getMetadata().getName() + ", " +
                    score + ")" );
            statement.execute("INSERT INTO ScheduleReport (pod_name, node_name, score) VALUES ('" + pod.getMetadata().getName() + "', '" + node.getMetadata().getName() + "', " +
                    score + ")" );

            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
