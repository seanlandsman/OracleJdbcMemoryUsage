package com.seanlandsman.jdbc;

import oracle.jdbc.pool.OracleDataSource;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class OracleJdbcMemoryUsage {
    public static void main(String[] args) throws SQLException {
        printBufferSizesForVariousQueries(10, 10);
        createStatementAllocateObjectsAndWait(60, 10000);
    }

    private static void createIntegers(int numberToCreate) {
        Random random = new Random();
        List<Integer> internalObjects = new ArrayList<Integer>();
        for (int i = 0; i < numberToCreate; i++) {
            internalObjects.add(random.nextInt());
        }
    }

    private static void printBufferSizesForVariousQueries(int initialFetchSize, int fetchMultiple) {
        String[] queries = {
                "select * from TAB",
                "select id from TAB",
                "select * from student"
        };

        for (String query : queries) {
            int fetchSize = initialFetchSize;

            System.out.format("For the following query: %s: \n", query);
            for (int i = 0; i < 4; i++) {
                printBufferSizesForFetchSizeOf(Integer.toString(fetchSize), query);
                fetchSize *= fetchMultiple;
            }
        }
    }

    private static void printBufferSizesForFetchSizeOf(String fetchSize, String query) {
        OracleDataSource ods = null;
        Connection conn = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            ods = new OracleDataSource();
            ods.setUser("student");
            ods.setPassword("student");
            ods.setDriverType("thin");
            ods.setServerName("localhost");
            ods.setPortNumber(1521);
            ods.setDatabaseName("xe");

            Properties properties = new Properties();
            properties.setProperty("defaultRowPrefetch", fetchSize);

            ods.setConnectionProperties(properties);
            conn = ods.getConnection();

            statement = conn.createStatement();
            resultSet = statement.executeQuery(query);

            printBufferSizes(statement);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeEverything(ods, conn, statement, resultSet);
        }
    }

    private static void printBufferSizes(Statement statement) {
        Class<? extends Statement> aClass = statement.getClass();
        try {
            Field statementField = aClass.getDeclaredField("statement");
            statementField.setAccessible(true);
            Object currentStatement = statementField.get(statement);
            Class<?> oracleStatementClass = currentStatement.getClass().getSuperclass();

            Field definesBatchSizeField = oracleStatementClass.getDeclaredField("definesBatchSize");
            definesBatchSizeField.setAccessible(true);
            Integer definesBatchSize = (Integer) definesBatchSizeField.get(currentStatement);

            Field defineBytesField = oracleStatementClass.getDeclaredField("defineBytes");
            defineBytesField.setAccessible(true);
            byte[] defineBytes = (byte[]) defineBytesField.get(currentStatement);

            Field defineCharsField = oracleStatementClass.getDeclaredField("defineChars");
            defineCharsField.setAccessible(true);
            char[] defineChars = (char[]) defineCharsField.get(currentStatement);

            System.out.format("batch size: %1$6d, defineBytes %2$15s, defineChars %3$15s\n", definesBatchSize, (defineBytes == null ? "null" : defineBytes.length + " long"), (defineChars == null ? "null" : defineChars.length + " long"));
        } catch (Exception ingored) {
        }
    }

    private static void createStatementAllocateObjectsAndWait(final int duration, int fetchSize) {
        OracleDataSource ods = null;
        Connection conn = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            ods = new OracleDataSource();
            ods.setUser("student");
            ods.setPassword("student");
            ods.setDriverType("thin");
            ods.setServerName("localhost");
            ods.setPortNumber(1521);
            ods.setDatabaseName("xe");

            Properties properties = new Properties();
            properties.setProperty("defaultRowPrefetch", Integer.toString(fetchSize));

            ods.setConnectionProperties(properties);
            conn = ods.getConnection();

            final Statement mystatement = conn.createStatement();
            final ResultSet myResultSet = mystatement.executeQuery("select * from student");

            final long start = System.currentTimeMillis();
            final CountDownLatch latch = new CountDownLatch(1);

            new Thread(new Runnable() {
                int i = 0;
                @Override
                public void run() {
                    long tenSeconds = TimeUnit.SECONDS.toMillis(duration);
                    while ((System.currentTimeMillis() - start) < tenSeconds) {
                        try {
                            createIntegers(200000);
                            Thread.sleep(TimeUnit.SECONDS.toMillis(1));

                            i++;
                            if (i == 10) {
                                try {

                                    System.out.println("closing");
                                    myResultSet.close();
                                } catch (SQLException e) {
                                }
//                                statement.close();
                            }
                            if (i==15) {
                                System.out.println("closing");
                                try {
                                    mystatement.close();
                                } catch (SQLException e) {

                                }
                            }

                        } catch (InterruptedException ignored) {
                        }
                    }
                    latch.countDown();
                }
            }).start();

            System.out.println("Waiting...");
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeEverything(ods, conn, statement, resultSet);
        }

    }

    private static void closeEverything(OracleDataSource ods, Connection conn, Statement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
            if (ods != null) {
                ods.close();
            }
        } catch (Exception ignored) {
        }
    }
}


