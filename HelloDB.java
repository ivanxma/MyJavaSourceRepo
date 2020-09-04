 

import java.sql.Connection;

import java.sql.DriverManager;

import java.sql.SQLException;

import java.sql.Statement;

import java.util.ArrayList;

 

public class HelloDB {

    private static long ttltime=0;

    private static int THREADSIZE=10;

    private static int ITERATIONS=10;

    private static int BATCHSIZE=10;

    private static String driver = "com.mysql.cj.jdbc.Driver";

    private static String database = "test";

    private static String baseUrl =   "jdbc:mysql://" +

        "address=(protocol=tcp)(type=master)(host=<MDS Private IP>)(port=3306)/" +

            database + "?verifyServerCertificate=false&useSSL=true&" +

            "loadBalanceConnectionGroup=first&loadBalanceEnableJMX=true";

    private static String user = "<MDS user>";

    private static String password = "<MDS Password>";

 

    public static void main(String[] args) throws Exception {

        createTable();

        ArrayList threads = new ArrayList(THREADSIZE);;

        for (int i=0;i<THREADSIZE;i++) {

            Thread t =  new Thread( new Repeater(i));

            t.start();

            threads.add(t);

        }

        System.out.println("Spawned threads : " + threads.size());

        for(int i=0;i<threads.size();i++) {

            ((Thread) threads.get(i)).join();

        }

        System.out.println("Finished - " + ttltime);

    }

 

    private static void createTable() throws ClassNotFoundException, SQLException {

        Connection c = getNewConnection();

         try {

                c.setAutoCommit(false);

                Statement s = c.createStatement();

                s.executeUpdate("create table if not exists test.mytable (f1 int auto_increment not null primary key, f2 varchar(200)) engine=innodb;");

                c.commit();

            } catch (SQLException e) {

                e.printStackTrace();

            }

        c.close();

    }

 

    static Connection getNewConnection( ) throws SQLException, ClassNotFoundException {

        java.util.Properties pp = new java.util.Properties();

        pp.setProperty("user", user);

        pp.setProperty("password", password);

        // black list for 60seconds

        pp.setProperty("loadBalanceBlacklistTimeout", "60000");

        pp.setProperty("autoReconnect", "false");

        Class.forName(driver);

        return DriverManager.getConnection(baseUrl, pp);

    }

 

    static void executeSimpleTransaction(Connection c, int conn, int trans){

        try {

            c.setReadOnly(false);

            c.setAutoCommit(false);

            Statement s = c.createStatement();

 

            s.executeUpdate("insert into test.mytable (f2) values ('Connection: " + conn + ", transaction: " + trans +"');" );

            c.commit();

        } catch (SQLException e) {

            e.printStackTrace();

        }

    }

 

     public static class Repeater implements Runnable {

            private int myid;

           public Repeater(int id) { myid=id; }

            public void run() {

                for(int i=0; i < ITERATIONS; i++){

                    System.out.println("Thread ID(" + String.valueOf(myid) + ") - Iteration(" + String.valueOf(i) + "/" + String.valueOf(ITERATIONS) + ")");

                    try {

                        Connection c = getNewConnection();

                        long mystart, myend, myttl=0;

                        for(int j=0; j < BATCHSIZE; j++){

                            // To register the start time

                            mystart = System.currentTimeMillis();

                            executeSimpleTransaction(c, i, j);

                            // To time the execution time and save it onto the totaltime

                            myend = System.currentTimeMillis();

                            myttl += (myend - mystart);

                            incTTL(myttl);

                        }

                        c.close();

                        Thread.sleep(200);

                    } catch (Exception e) {

                        e.printStackTrace();

                    }

                }

            }

        }

 

     public synchronized static void incTTL(long m) {

         ttltime += m;

     }

}

 
