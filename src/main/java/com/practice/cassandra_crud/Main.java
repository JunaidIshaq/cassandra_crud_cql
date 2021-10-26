package com.practice.cassandra_crud;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class Main {

    static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOGGER.info("Entered in Main Method");
        // TO DO: Fill in your own host, port, and data center
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withKeyspace("ascend_dev")
                .withLocalDatacenter("datacenter1")
                .build()) {
            LOGGER.info("Connection Successful !!!!!!!!!");

            setUser(session, 6,"Jones", "35", "Austin", "bob@example.com", "Bob");

            getUser(session, "ishaq");

            updateUser(session, "28", 1);

            getUser(session, "Jones");

            deleteUser(session, 1);

        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setUser(CqlSession session, int id, String lastname, String age, String city, String email, String firstname) {
        LOGGER.info("Entered in method setUser");
        //TO DO: execute SimpleStatement that inserts one user into the table
        String query = "INSERT INTO user (id, lastname, age, city, email, firstname) VALUES (?, ? , ? , ? , ? , ?)";
        LOGGER.debug("Insert Query : " + query);
        session.execute(
                SimpleStatement.builder( query)
                        .addPositionalValues(id, lastname, age, city, email, firstname)
                        .build());
        LOGGER.info("Exiting from method setUser");
    }

    private static void getUser(CqlSession session, String lastname) {
        LOGGER.info("Entered in method getUser");
        //TO DO: execute SimpleStatement that retrieves one user from the table
        //TO DO: print firstname and age of user
        ResultSet rs = session.execute(
                SimpleStatement.builder("SELECT * FROM user WHERE lastname = ? allow filtering")
                        .addPositionalValue(lastname)
                        .build());

        Row row = rs.one();
        System.out.format("%s %s\n", row.getString("firstname"), row.getString("age"));
        LOGGER.info("Existed from method getUser");
    }


    private static void updateUser(CqlSession session, String age, int id) {

        //TO DO: execute SimpleStatement that updates the age of one user
        session.execute(
                SimpleStatement.builder("UPDATE user SET age =?  WHERE id =? ")
                        .addPositionalValues(age, id)
                        .build());
    }

    private static void deleteUser(CqlSession session, int id) {

        //TO DO: execute SimpleStatement that deletes one user from the table
        session.execute(
                SimpleStatement.builder("delete from user where id=?")
                        .addPositionalValue(id)
                        .build());

    }
}
