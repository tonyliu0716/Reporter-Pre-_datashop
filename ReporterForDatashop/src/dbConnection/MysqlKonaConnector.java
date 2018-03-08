package dbConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlKonaConnector {
	public static Connection getConnection() throws Exception {

        Connection conn;
        conn = null;

       // JDBC driver name and database URL： mysql -u root -h 165.91.232.82 -P 8002 -D mysql -p
 	   
       // for kenya
 	   //final String DB_URL = "jdbc:mysql://165.91.232.82:8002";
 	   
       // for local testing
       final String DB_URL = "jdbc:mysql://kona.education.tamu.edu:3306";
 	   //  Database credentials
 	   final String USER = "simstudent";
 	   final String PASS = "simstudent";

        try {
            //SSH loging username

        	//STEP 2: Register JDBC driver
		      Class.forName("com.mysql.jdbc.Driver");
	
		      //STEP 3: Open a connection
		      System.out.println("Connecting to Kona SMS database...");
		      conn = DriverManager.getConnection(DB_URL,USER,PASS);


        } catch (Exception e) {
            MysqlSSHConnector.closeConnection(conn, null);
            e.printStackTrace();

        }

        return conn;

	}

	public static void closeConnection(Connection conn, PreparedStatement ps) throws Exception {
		
		if(ps != null) {
	        ps.close();
	    }
	
	    if(conn != null) {
	        try {
	            conn.close();
	        } catch(SQLException e) {
	            e.printStackTrace();
	            throw e;
	        }
	    }
	    
	
	    System.out.println("Kona SMS Database has been closed..");
	}
}

