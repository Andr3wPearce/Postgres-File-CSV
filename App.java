import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App {
    private static final String DB_TABLE = "players";

    public static void main(String[] args) {
        Connection c = getDatabase(getDB_URL());
        if (c != null)
            System.out.println("Connected to database");
        else {
            System.out.println("Failed to connect to database");
            System.exit(-2);
        }

    }

    private static String getDB_URL() {
        Map<String, String> env = System.getenv();
        String db_url = env.get("DATABASE_URL");
        if (db_url != null) {
            return db_url;
        } else {
            System.out.println("DATABASE_URL not found. Please set the DATABASE_URL environment variable.");
            System.exit(-1);
            return null;
        }
    }

    /**
     * Get a fully-configured connection to the database
     * 
     * @param db_url the url of the Heroku Postgres database server
     * 
     * @return A Database object, or null if we cannot connect properly
     */
    static Connection getDatabase(String db_url) {

        // Give the Database object a connection, fail if we cannot get one
        try {
            Class.forName("org.postgresql.Driver");
            URI dbUri = new URI(db_url);
            String username = dbUri.getUserInfo().split(":")[0];
            String password = dbUri.getUserInfo().split(":")[1];
            String dbUrl = "jdbc:postgresql://" + dbUri.getHost() + ':' + dbUri.getPort() + dbUri.getPath();
            if (dbUrl.charAt(dbUrl.length() - 1) != '/')
                dbUrl += '/';
            Connection conn = DriverManager.getConnection(dbUrl, username, password);
            if (conn == null) {
                System.err.println("Error: DriverManager.getConnection() returned a null object");
                return null;
            }
            return conn;
        } catch (SQLException e) {
            System.err.println("Error: DriverManager.getConnection() threw a SQLException");
            e.printStackTrace();
            return null;
        } catch (ClassNotFoundException cnfe) {
            System.out.println("Unable to find postgresql driver");
            return null;
        } catch (URISyntaxException s) {
            System.out.println("URI Syntax Error");
            return null;
        }

    }
}
