# Postgres-File-CSV
A simple Java program to create a database from a CSV file, using input from terminal for certain variable lengths

When compiling, it is necessary to include the [postgres JDBC driver](https://jdbc.postgresql.org) in the class path. You must also make sure to specify the databse url as an enviroment variable. Do this by using a similar command to the following:
`DATABASE_URL=postgres://postgres:postgrespw@host:55001 java -cp ".:./postgresql-42.5.0.jar" App.java`
