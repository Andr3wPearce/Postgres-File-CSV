import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class App {

    public static void main(String[] args) {
        int sum = 0;
        Connection c = getDatabase(getDB_URL());
        if (c != null)
            System.out.println("Connected to database");
        else {
            System.out.println("Failed to connect to database");
            System.exit(-2);
        }
        File f = null;
        Scanner sc = new Scanner(System.in);
        System.out.println("Please specify a name for the database table: ");
        String tableName = sc.nextLine();
        try {
            c.prepareStatement("DROP TABLE IF EXISTS " + tableName).executeQuery();
            System.out.println("Table " + tableName + " dropped");
        } catch (SQLException e1) {
            // Table didn't exist
        }
        do {
            if (f != null) {
                System.out.println("File not found. Please try again.");
            }

            System.out.println("Enter file name: ");
            String fileName = sc.nextLine();
            f = new File(fileName);
        } while (!f.exists());
        System.out.println("File found");
        Map<String, String> l = new HashMap<>();
        try (Scanner fileRead = new Scanner(f)) {
            // Get headers
            String firstLine = fileRead.nextLine();
            String[] headers = firstLine.split(",");
            // Loop through headers to find the type of each column
            System.out.println("Please enter the SQL type(varChar(n), numeric(), bool, ...) of each column");
            for (int o = 0; o < headers.length; o++) {
                String s = headers[o];
                System.out.println(s + " is of SQL type: ");
                // Formats all input to lower case
                s.toLowerCase();
                // Formats all input to remove spaces
                s = s.replaceAll("\\s+", "");
                // Adds tablename_ to the front of each column name
                s = tableName + "_" + s;
                headers[o] = s;
                String type = sc.nextLine();
                l.put(s, type);
            }
            // Build SQL statement
            String statement = createQuery(tableName, l);
            System.out.println("Executing statement: " + statement);
            // Create table
            c.prepareStatement(statement).executeUpdate();
            // Insert data
            while (fileRead.hasNextLine()) {
                String q;
                String line = fileRead.nextLine();
                String[] data = line.split(",");
                q = "INSERT INTO " + tableName + "(";
                for (int m = 0; m < headers.length; m++) {
                    q += headers[m];
                    if (m != headers.length - 1)
                        q += ", ";
                }
                q = q + ") VALUES (";
                for (int i = 0; i < data.length; i++) {
                    if (data[i].equals("") || data[i].equals(" ")) {
                        q += "NULL";
                        continue;
                    }
                    try {
                        Integer k = Integer.parseInt(data[i]);
                        q += data[i];
                    } catch (NumberFormatException e) {
                        q += "'" + data[i] + "'";
                    }
                    if (i != data.length - 1)
                        q += ",";
                }
                q += ")";
                System.out.println("Executing statement: " + q);
                sum += c.prepareStatement(q).executeUpdate();
            }
        } catch (FileNotFoundException e) {
            System.out.println("Error reading file");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Error creating table");
            e.printStackTrace();
        }
        sc.close();
        System.out.println("Inserted " + sum + " rows");
        try {
            c.close();
        } catch (SQLException e) {
            System.out.println("Error closing connection");
            e.printStackTrace();
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

    private static String createQuery(String tableName, Map<String, String> columns) {
        String query = "CREATE TABLE " + tableName + " (";
        for (int i = 0; i < columns.size(); i++) {
            String key = columns.keySet().toArray()[i].toString();
            String value = columns.get(key);
            query += key + " " + value;
            if (i != columns.size() - 1) {
                query += ", ";
            }
        }
        query += ");";
        return query;
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
