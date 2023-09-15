package com.ipl.mysql;

import io.github.cdimascio.dotenv.Dotenv;
import java.sql.*;

public class Main {

    static Dotenv dotenv = Dotenv.configure().load();
    private static String DB_MYSQL_DRIVER = dotenv.get("DB_MYSQL_DRIVER")
    private static String DB_MYSQL_URL= dotenv.get("DB_MYSQL_URL");
    private static String DB_MYSQL_USERNAME = dotenv.get("DB_MYSQL_USERNAME");
    private static String DB_MYSQL_PASSWORD = dotenv.get("DB_MYSQL_PASSWORD");


    public static void main(String[] args) {
        findNumberOfMatchesPlayedPerYear();
        findNumberOfMatchesWonByEachTeam();
        findExtraRunsConcededPerTeam();
        findMostEconomicalBowlerGivenYear();
    }


    private static void findMostEconomicalBowlerGivenYear() {
        String query = "select d.bowler, (sum(d.total_runs - d.bye_runs - d.legbye_runs) / (count(case when (d.wide_runs = 0 and d.noball_runs = 0) then 1 else null end) / 6.0)) as economy_rate from deliveries d inner join matches m on d.match_id = m.id where m.season = 2015 group by d.bowler order by economy_rate limit 10; ";

        try {
            Class.forName(DB_MYSQL_DRIVER);
            Connection connection = DriverManager.getConnection(DB_MYSQL_URL, DB_MYSQL_USERNAME, DB_MYSQL_PASSWORD);
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(query);

            System.out.printf("\n%-30s %s\n", "bowler", "economy_rate");
            System.out.println("--------------------------------------");

            while(rs.next()) {
                String bowler = rs.getString("bowler");
                double economyRate= rs.getDouble("economy_rate");
                System.out.printf("%-30s %s\n", bowler, economyRate);
            }

            if(!connection.isClosed()) connection.close();

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void findExtraRunsConcededPerTeam() {

        String query = "select d.bowling_team, sum(d.extra_runs) as extra_runs from deliveries d inner join matches m on d.match_id = m.id where m.season = 2016 group by d.bowling_team order by extra_runs";

        try {
            Class.forName(DB_MYSQL_DRIVER);
            Connection connection = DriverManager.getConnection(DB_MYSQL_URL, DB_MYSQL_USERNAME, DB_MYSQL_PASSWORD);
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(query);

            System.out.printf("\n%-30s %s\n", "team", "extra_runs");
            System.out.println("--------------------------------------");

            while(rs.next()) {
                String team = rs.getString("bowling_team");
                int extraRuns= rs.getInt("extra_runs");
                System.out.printf("%-30s %s\n", team, extraRuns);
            }

            if(!connection.isClosed()) connection.close();

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void findNumberOfMatchesWonByEachTeam() {

        String query = "select m.winner, count(*) as matches_won from matches m where winner is Not Null group by m.winner order by matches_won";

        try {
            Class.forName(DB_MYSQL_DRIVER);
            Connection connection = DriverManager.getConnection(DB_MYSQL_URL, DB_MYSQL_USERNAME, DB_MYSQL_PASSWORD);
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(query);

            System.out.printf("\n%-30s %s\n", "team", "matches_won");
            System.out.println("--------------------------------------");

            while(rs.next()) {
                String team = rs.getString("winner");
                String matches_won = rs.getString("matches_won");
                System.out.printf("%-30s %s\n", team, matches_won);

            }

            if(!connection.isClosed()) connection.close();

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void findNumberOfMatchesPlayedPerYear() {

        String query = "select m.season, count(*) as matches_played from matches m group by m.season order by m.season;";

        try {
            Class.forName(DB_MYSQL_DRIVER);
            Connection connection = DriverManager.getConnection(DB_MYSQL_URL, DB_MYSQL_USERNAME, DB_MYSQL_PASSWORD);
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(query);

            System.out.printf("\n%-30s %s\n", "season", "matches_played");
            System.out.println("--------------------------------------");

            while(rs.next()) {
                String season = rs.getString("season");
                int matches_played = rs.getInt("matches_played");
                System.out.printf("%-30s %s\n", season, matches_played);
            }

            if(!connection.isClosed()) connection.close();

        } catch(Exception e) {
            e.printStackTrace();
        }

    }



}