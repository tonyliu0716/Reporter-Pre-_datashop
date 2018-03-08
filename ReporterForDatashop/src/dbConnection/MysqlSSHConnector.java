package dbConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlSSHConnector {
	public static Connection getConnection() throws Exception {

        Connection conn;
        conn = null;

       // JDBC driver name and database URL： mysql -u root -h 165.91.232.82 -P 8002 -D mysql -p
 	   
       // for kenya
 	   final String DB_URL = "jdbc:mysql://165.91.232.82:8002?useUnicode=true&characterEncoding=UTF-8";
 	   
       // for local testing
       //final String DB_URL = "jdbc:mysql://127.0.0.1:8002";
 	   //  Database credentials
 	   final String USER = "root";
 	   final String PASS = "";

        try {
            //SSH loging username

        	//STEP 2: Register JDBC driver
		      Class.forName("com.mysql.jdbc.Driver");
	
		      //STEP 3: Open a connection
		      System.out.println("Connecting to Kenya OpenedX database...");
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
	    
	
	    System.out.println("Kenya OpenedX Database has been closed..");
	}
}
