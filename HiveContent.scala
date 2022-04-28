// Run on VM as spark-submit
package Project

import java.io.IOException
import scala.util.Try

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

//import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.session.SessionState
import java.sql.Driver

object HiveContent {
    def main(args: Array[String]) {
        println("Starting Hive Content...")

        content()
    }

def content(): Unit = {

    var con: java.sql.Connection = null;
    try {
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";

        Class.forName(driverName);

        con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement();
        stmt.executeQuery("Show databases");
        System.out.println("show database successfully");

        println("Executing SELECT * FROM insurance..")
        var res = stmt.executeQuery("SELECT * FROM insurance")
        while (res.next()) {
            println(s"${res.getString(1)}, ${res.getString(3)}, ${res.getString(4)}")
        }

        val tableName = "testHiveDriverTable";
        println(s"Dropping table $tableName..")
        stmt.execute("drop table IF EXISTS " + tableName);
        println(s"Creating table $tableName..")
        stmt.execute(
            "create table " + tableName + " (key int, value string) row format delimited  fields terminated by ','");

        //Show Tables
        println(s"Show TABLES $tableName..")
        var sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }

        //Describe Tables
        println(s"Describing table $tableName..")
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        //Load Data Into Table
        // NOTE: Filepath has to be local to the hive server.
        val filepath = "/tmp/insurance.csv";
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);

        //SELECT * Query
        sql = "SELECT * FROM " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(
            String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
        }

        //Regular Hive Query
        sql = "SELECT COUNT(1) FROM " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));}
        } 
        catch {
            case ex: Throwable => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")}
            } 
        finally {
            try {
                if (con != null)
                con.close();
                } 
            catch {
                case ex: Throwable => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")
        }
      }
    }
  }
}

