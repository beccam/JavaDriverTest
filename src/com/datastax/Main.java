package com.datastax;

import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.datastax.driver.core.*;

import java.util.Iterator;

public class Main {

    static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("Starting driver tests");

        Cluster cluster;
        Session session;
        ResultSet results;
        Row rows;

        // Connect to the cluster and keyspace "demo"
        cluster = Cluster
                .builder()
                .addContactPoint("localhost")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                .build();
        session = cluster.connect("demo");

        // Insert one record into the users table
        PreparedStatement statement = session.prepare(

                "INSERT INTO users" + "(lastname, age, city, email, firstname)"
                        + "VALUES (?,?,?,?,?);");

        BoundStatement boundStatement = new BoundStatement(statement);

        session.execute(boundStatement.bind("Jones", 35, "Austin",
                "bob@example.com", "Bob"));

        // Use select to get the user we just entered
        Statement select = QueryBuilder.select().all().from("demo", "users")
                .where(QueryBuilder.eq("lastname", "Jones"));
        results = session.execute(select);
        for (Row row : results) {
            System.out.format("%s %d \n", row.getString("firstname"),
                    row.getInt("age"));
        }

        // Update the same user with a new age
        Statement update = QueryBuilder.update("demo", "users")
                .with(QueryBuilder.set("age", 36))
                .where((eq("lastname", "Jones")));
        session.execute(update);

        // Select and show the change
        select = QueryBuilder.select().all().from("demo", "users")
                .where(eq("lastname", "Jones"));
        results = session.execute(select);
        for (Row row : results) {
            System.out.format("%s %d \n", row.getString("firstname"),
                    row.getInt("age"));
        }

        //Prepared statement with Query Builder example
        PreparedStatement state = session.prepare(QueryBuilder.select().all().from("demo", "users").where(eq("lastname", bindMarker())));

        BoundStatement boundState = new BoundStatement(state);

        results = session.execute(boundState.bind("Jones"));

        for (Row row : results) {
            System.out.format("%s %d \n", row.getString("firstname"),
                    row.getInt("age"));
        }

        // Delete the user from the users table
        Statement delete = QueryBuilder.delete().from("users")
                .where(eq("lastname", "Jones"));
        results = session.execute(delete);


        // Show that the user is gone
        select = QueryBuilder.select().all().from("demo", "users");
        results = session.execute(select);
        for (Row row : results) {
            System.out.format("%s %d %s %s %s\n", row.getString("lastname"),
                    row.getInt("age"), row.getString("city"),
                    row.getString("email"), row.getString("firstname"));
        }

        //Let's try a batch
        session.execute(
                "BEGIN BATCH" +
                "   INSERT INTO demo.users (lastname, age, city, email, firstname) VALUES ('Smith', 46, 'Sacramento', 'john@example.com', 'John'); " +
                "   INSERT INTO demo2.users (lastname, age, city, email, firstname) VALUES ('Doe', 36, 'Beverly Hills', 'jane@example.com', 'Jane'); " +
                "   INSERT INTO demo3.users (lastname, age, city, email, firstname) VALUES ('Byrne', 24, 'San Diego', 'rob@example.com', 'Rob'); " +
                "APPLY BATCH"
                );

        Statement stmt = new SimpleStatement("Select * from users");
        stmt.setFetchSize(2);
        ResultSet rs = session.execute(stmt);

/*        ResultSet rs = session.execute(...);
        Iterator<Row> iter = rs.iterator();
        while (iter.hasNext()) {
            if (rs.getAvailableWithoutFetching() == 2 && !rs.isFullyFetched())
                rs.fetchMoreResults();
            Row row = iter.next();
         //   ...process the row...
        }*/

        // Clean up the connection by closing it
        cluster.close();
    }
}

