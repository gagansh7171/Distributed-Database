// A Java program for a Server
import java.net.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import org.javatuples.Triplet;
public class Server
{
    //initialize socket and input stream
    private Socket          socket   = null;
    private ServerSocket    server   = null;
    private DataInputStream in       =  null;
	private DataOutputStream out     = null;
    private String QueryResult = "";
    private Connection connection;
    private int NumMachines = 1;
    private int NumDatabases = 4;
    private ArrayList<Triplet<String, String, String>> DBInstance = new ArrayList<Triplet<String, String, String>>();
    private Connection[][] DBConnection = new Connection[NumMachines][NumDatabases];

    private void prepareResultSet(ResultSet rs){
        if(QueryResult.length()>5 && QueryResult.substring(0,5).equals("Error"))
            return;
        try {
            if (rs == null) {
                QueryResult = "Error: Result set is null!";
                return;
            }
            if (rs.isClosed()) {
                QueryResult = "Error: Result Set is closed!";
                return;
            }

            // Get the meta data object of this ResultSet.
            ResultSetMetaData rsmd;
            rsmd = rs.getMetaData();

            // Total number of columns in this ResultSet
            int columnCount = rsmd.getColumnCount();
            if(QueryResult.length() == 0){
                for (int i = 1; i <= columnCount; i++) {
                    QueryResult += rsmd.getColumnLabel(i) +",";
                }
                QueryResult += ";";
            }
            while(rs.next()){
                for (int i = 0; i < columnCount; i++) {
                    String value = rs.getString(i+1) == null ? "NULL" : rs.getString(i+1);
                    QueryResult += value +",";
                }
                QueryResult += ";";
            }
        } catch (SQLException e) {
            QueryResult = "Error: " +e.getMessage();
        }
    }
    private void insert(String q) {
        try{
            int i = q.indexOf("VALUES");
            if(i == -1){
                QueryResult = "Error: Syntax error in SQL query";
                out.writeUTF(QueryResult);
                return;
            }
            while(i<q.length() && !Character.isDigit(q.charAt(i))) i++;
            int j = i;
            while(j < q.length() && Character.isDigit(q.charAt(j))) j++;
            if(i == q.length()){
                QueryResult = "Error: Syntax error in SQL query";
                out.writeUTF(QueryResult);
                return;
            }
            int uuid = Integer.parseInt(q.substring(i, j), 10);
            int db = uuid%4;
            int machine = (uuid/4)%4 ;
            
            int rows = DBConnection[0][db].createStatement().executeUpdate(q);
            QueryResult = "INSERT: "+rows+" row changed";
            out.writeUTF(QueryResult);
        }
        catch(SQLSyntaxErrorException ex)
        {
            try{
                out.writeUTF("Error: " +ex.getMessage());
            }
            catch(IOException i)
            {
                System.out.println(ex.getMessage());
            }
        }
        catch(SQLException ex)
        {
            try{
                out.writeUTF("Error: " +ex.getMessage());
            }
            catch(IOException i)
            {
                System.out.println(ex.getMessage());
            }
        }
        catch(IOException i)
        {
            System.out.println(i);
        }
        return;
    }
    private void select(String q) {
        try{
            if(q.contains("UUID")){
                int i = q.indexOf("UUID");
                if(i == -1){
                    QueryResult = "Error: Syntax error in SQL query";
                    out.writeUTF(QueryResult);
                    return;
                }
                while(i<q.length() && !Character.isDigit(q.charAt(i))) i++;
                int j = i;
                while(j < q.length() && Character.isDigit(q.charAt(j))) j++;
                if(i == q.length()){
                    QueryResult = "Error: Syntax error in SQL query";
                    out.writeUTF(QueryResult);
                    return;
                }
                int uuid = Integer.parseInt(q.substring(i, j), 10);
                int db = uuid%NumDatabases;
                int machine = (uuid/NumDatabases)%NumMachines ;
                ResultSet result = DBConnection[0][db].createStatement().executeQuery(q);
                prepareResultSet(result);
                out.writeUTF(QueryResult);
            } else {
                SelectQuery queries[] = new SelectQuery[NumDatabases];
                for(int i = 0 ; i<NumMachines; i++){
                    for(int j = 0;  j<NumDatabases; j++){
                        queries[j] = new SelectQuery(q, DBConnection[i][j]);
                        queries[j].start();
                    }
                }
                for(int i = 0 ; i<NumMachines; i++){
                    for(int j = 0;  j<NumDatabases; j++){
                        try{
                            queries[j].join();
                            if(queries[j].error.equals("None")){
                                prepareResultSet(queries[j].result);
                            }
                            else if(queries[j].error.equals("SQLSyntaxErrorException")){
                                throw new SQLSyntaxErrorException();
                            }
                            else if(queries[j].error.equals("SQLException")){
                                throw new SQLException();
                            }
                        } catch(InterruptedException exc){
                            System.out.println(exc.getMessage());
                        }
                    }
                }
                out.writeUTF(QueryResult);
            }
        }
        catch(SQLSyntaxErrorException ex){
            try{
                out.writeUTF("Error: " +ex.getMessage());
            }
            catch(IOException i)
            {
                System.out.println(ex.getMessage());
            }
        }
        catch(SQLException ex)
        {
            try{
                out.writeUTF("Error: " +ex.getMessage());
            }
            catch(IOException i)
            {
                System.out.println(ex.getMessage());
            }
        }
        catch(IOException i)
        {
            System.out.println(i);
        }
        return;
    }
    private void update(String q) {
        try{
            
            String[] splitted = q.split("WHERE");
            String condition;
            if(q.contains("WHERE")){
                condition  = splitted[1].trim();
            } else {
                condition = "1=1";
            }
            String[] splitted2 = splitted[0].split("SET");
            String[] set = splitted2[1].split(",");
            String[] orderedset = {"-1", "-1", "-1"};
            for(int i = 0; i< set.length; i++){
                String[] pair = set[i].split("=");
                if(pair[0].trim().equals("FIRSTNAME")){
                    orderedset[0] = pair[1].trim();
                }
                else if(pair[0].trim().equals("SECONDNAME")){
                    orderedset[1] = pair[1].trim();
                }
                else if(pair[0].trim().equals("UUID")){
                    orderedset[2] = pair[1].trim();
                }
            }
            String table = splitted2[0].split("UPDATE")[1].trim();
            if(!orderedset[2].equals("-1")){
                String selectQuery = "SELECT * from "+table+" Where "+condition;
                ArrayList<Triplet<String,String,String>> selectResult = new ArrayList<Triplet<String,String,String>>();
                SelectQuery squeries[] = new SelectQuery[NumDatabases];
                for(int i = 0 ; i<NumMachines; i++){
                    for(int j = 0;  j<NumDatabases; j++){
                        squeries[j] = new SelectQuery(selectQuery, DBConnection[i][j]);
                        squeries[j].start();
                    }
                }
                for(int i = 0 ; i<NumMachines; i++){
                    for(int j = 0;  j<NumDatabases; j++){
                        try{
                            squeries[j].join();
                            if(squeries[j].error.equals("None")){
                                ResultSet rs = squeries[j].result;
                                if (rs != null && !rs.isClosed()) {
                                    while(rs.next()){
                                        selectResult.add(new Triplet<String, String, String>(
                                            orderedset[0].equals("-1")? (rs.getString(2) == null ? "NULL" : rs.getString(2)) : orderedset[0], 
                                            orderedset[1].equals("-1")? (rs.getString(3) == null ? "NULL" : rs.getString(3)) : orderedset[1], 
                                            orderedset[2].equals("-1")? (rs.getString(4) == null ? "NULL" : rs.getString(4)) : orderedset[2]
                                        ));
                                    }
                                }
                            }
                            else if(squeries[j].error.equals("SQLSyntaxErrorException")){
                                throw new SQLSyntaxErrorException();
                            }
                            else if(squeries[j].error.equals("SQLException")){
                                throw new SQLException();
                            }
                        } catch(InterruptedException exc){
                            System.out.println(exc.getMessage());
                        }
                    }
                }
                int rows = 0;
                String deleteQuery = "DELETE FROM "+table+" Where "+condition;

                UpdateQuery queries[] = new UpdateQuery[NumDatabases];
                for(int i = 0 ; i<NumMachines; i++){
                    for(int j = 0;  j<NumDatabases; j++){
                        queries[j] = new UpdateQuery(deleteQuery, DBConnection[i][j]);
                        queries[j].start();
                    }
                }
                for(int i = 0 ; i<NumMachines; i++){
                    for(int j = 0;  j<NumDatabases; j++){
                        try{
                            queries[j].join();
                            if(queries[j].error.equals("None")){
                                rows += queries[j].rows;
                            }
                            else if(queries[j].error.equals("SQLSyntaxErrorException")){
                                throw new SQLSyntaxErrorException();
                            }
                            else if(queries[j].error.equals("SQLException")){
                                throw new SQLException();
                            }
                        } catch(InterruptedException exc){
                            System.out.println(exc.getMessage());
                        }
                    }
                }
                int uuid = Integer.parseInt(orderedset[2],10);
                int db = uuid%4;
                int machine = (uuid/4)%4 ;
                for(int i = 0; i<rows; i++){
                    String insertQuery = "INSERT INTO "+table+"(firstName, secondName, uuid) Values('"+selectResult.get(i).getValue0()+"', '"+selectResult.get(i).getValue1()+"', '"+selectResult.get(i).getValue2()+"') ";
                    DBConnection[0][db].createStatement().executeUpdate(insertQuery);
                }
                QueryResult = "UPDATE: "+rows+" row changed";
                out.writeUTF(QueryResult);
            }
            else{
                int rows = 0;
                UpdateQuery queries[] = new UpdateQuery[NumDatabases];
                for(int i = 0 ; i<NumMachines; i++){
                    for(int j = 0;  j<NumDatabases; j++){
                        queries[j] = new UpdateQuery(q, DBConnection[i][j]);
                        queries[j].start();
                    }
                }
                for(int i = 0 ; i<NumMachines; i++){
                    for(int j = 0;  j<NumDatabases; j++){
                        try{
                            queries[j].join();
                            if(queries[j].error.equals("None")){
                                rows += queries[j].rows;
                            }
                            else if(queries[j].error.equals("SQLSyntaxErrorException")){
                                throw new SQLSyntaxErrorException();
                            }
                            else if(queries[j].error.equals("SQLException")){
                                throw new SQLException();
                            }
                        } catch(InterruptedException exc){
                            System.out.println(exc.getMessage());
                        }
                    }
                }
                QueryResult = "UPDATE: "+rows+" row"+((rows>1)?"s":"")+" changed";
                out.writeUTF(QueryResult);
            }
        }
        catch(SQLSyntaxErrorException ex){
            try{
                out.writeUTF("Error: " +ex.getMessage());
            }
            catch(IOException i)
            {
                System.out.println(ex.getMessage());
            }
        }
        catch(SQLException ex)
        {
            try{
                out.writeUTF("Error: " +ex.getMessage());
            }
            catch(IOException i)
            {
                System.out.println(ex.getMessage());
            }
        }
        catch(IOException i)
        {
            System.out.println(i);
        }
        return;
    }
    private void delete(String q) {
        try{
            if(q.contains("UUID")){
                int i = q.indexOf("UUID");
                if(i == -1){
                    QueryResult = "Syntax error in SQL query";
                    return;
                }
                while(i<q.length() && !Character.isDigit(q.charAt(i))) i++;
                int j = i;
                while(j < q.length() && Character.isDigit(q.charAt(j))) j++;
                if(i == q.length()){
                    QueryResult = "Syntax error in SQL query";
                    return;
                }
                int uuid = Integer.parseInt(q.substring(i, j), 10);
                int db = uuid%4;
                int machine = (uuid/4)%4 ;

                int rows = DBConnection[0][db].createStatement().executeUpdate(q);
                QueryResult = "DELETE: "+rows+" row"+((rows>1)?"s":"")+" deleted";
                out.writeUTF(QueryResult);
            } else {
                int rows = 0;
                UpdateQuery queries[] = new UpdateQuery[NumDatabases];
                for(int i = 0 ; i<NumMachines; i++){
                    for(int j = 0;  j<NumDatabases; j++){
                        queries[j] = new UpdateQuery(q, DBConnection[i][j]);
                        queries[j].start();
                    }
                }
                for(int i = 0 ; i<NumMachines; i++){
                    for(int j = 0;  j<NumDatabases; j++){
                        try{
                            queries[j].join();
                            if(queries[j].error.equals("None")){
                                rows += queries[j].rows;
                            }
                            else if(queries[j].error.equals("SQLSyntaxErrorException")){
                                throw new SQLSyntaxErrorException();
                            }
                            else if(queries[j].error.equals("SQLException")){
                                throw new SQLException();
                            }
                        } catch(InterruptedException exc){
                            System.out.println(exc.getMessage());
                        }
                    }
                }
                QueryResult = "DELETE: "+rows+" row"+((rows>1)?"s":"")+" deleted";
                out.writeUTF(QueryResult);
            }
        }
        catch(SQLSyntaxErrorException ex)
        {
            try{
                out.writeUTF("Error: " +ex.getMessage());
            }
            catch(IOException i)
            {
                System.out.println(ex.getMessage());
            }
        }
        catch(SQLException ex)
        {
            try{
                out.writeUTF("Error: " +ex.getMessage());
            }
            catch(IOException i)
            {
                System.out.println(ex.getMessage());
            }
        }
        catch(IOException i)
        {
            System.out.println(i);
        }

        return;
    }
    private void describe(String q){
        try{
            ResultSet result = DBConnection[0][0].createStatement().executeQuery(q);
            prepareResultSet(result);
            out.writeUTF(QueryResult);
        }
        catch(SQLSyntaxErrorException ex){
            try{
                out.writeUTF("Error: " +ex.getMessage());
            }
            catch(IOException i)
            {
                System.out.println(ex.getMessage());
            }
        }
        catch(SQLException ex)
        {
            try{
                out.writeUTF("Error: " +ex.getMessage());
            }
            catch(IOException i)
            {
                System.out.println(ex.getMessage());
            }
        }
        catch(IOException i)
        {
            System.out.println(i.getMessage());
        }
    }
    public Server(int port)
    {
        // starts server and waits for a connection
        try
        {
            server = new ServerSocket(port);
            System.out.println("Server started");

            // list of machines and databases over which our data is distributed
            // stores database address, useername and password
            for(int i = 0 ; i<NumMachines; i++){
                DBInstance.add(new Triplet<String, String, String>("localhost:3306", "root", "helloiitr"));
            }
            // connect to database
            for(int i = 0 ; i<NumMachines; i++){
                for(int j = 1;  j<=NumDatabases; j++){
                    connection = DriverManager.getConnection("jdbc:mysql://"+DBInstance.get(i).getValue0()+"/iitrdb_"+j, DBInstance.get(i).getValue1(), DBInstance.get(i).getValue2());
                    DBConnection[i][j-1] = connection;
                }
            }

            while(true)
            {
                System.out.println("Waiting for a client ...");
                socket = server.accept();
                System.out.println("Client accepted");
    
                // takes input from the client socket
                in = new DataInputStream( new BufferedInputStream(socket.getInputStream()));
                // gives output to the client socket
                out = new DataOutputStream(socket.getOutputStream());
                String line = "";

                // reads message from client until "Over" is sent
                try {
                    
                    while (true)
                    {
                        line = in.readUTF();
                        QueryResult = "";
                        line = line.strip().toUpperCase();
                        System.out.println("Query: "+line);
                        if(line.strip().toUpperCase().equals("OVER") || line.strip().toUpperCase().equals("OVER AND OUT")){
                            break;
                        }
                        if(line.length() >= 6)
                        {
                            if(line.substring(0, 6).equals("INSERT")) {
                                insert(line);
                            }
                            else if(line.substring(0, 6).equals("SELECT")) {
                                select(line);
                            }
                            else if(line.substring(0, 6).equals("UPDATE")) {
                                update(line);
                            }
                            else if(line.substring(0, 6).equals("DELETE")) {
                                delete(line);
                            }
                            else if(line.substring(0, 4).equals("DESC")){
                                describe(line);
                            }
                            else{
                                out.writeUTF("Error: Query not supported.");
                            }
                        }else{
                            out.writeUTF("Error: Query not supported.");
                        }
                    }
                }
                catch(EOFException ex)
                {
                    System.out.println(line);
                }
                catch(IOException i)
                {
                    System.out.println(i);
                }

                // close connection
                System.out.println("Closing connection");
                socket.close();
                in.close();
                if(line.strip().toUpperCase().equals("OVER AND OUT"))
                    break;
            }
            System.out.println("Closing Server Socket");
            // close all database connection
            for(int i = 0 ; i<NumMachines; i++){
                for(int j = 0;  j<NumDatabases; j++){
                    DBConnection[i][j].close();
                }
            }
            // close server socket
            server.close();
            System.out.print("Done");
        }
        catch(IOException i)
        {
            System.out.println(i);
        }
        catch(SQLException ex)
        {
            try{
                out.writeUTF("Error: " +ex.getMessage());
            }
            catch(IOException i)
            {
                System.out.println(ex.getMessage());
            }
        }
    }
  
    public static void main(String args[])
    {
        Server server = new Server(5000);
    }
}

class SelectQuery extends Thread  {
    public ResultSet result;
    public String q;
    public Connection dbcon;
    public String error;
    SelectQuery(String q, Connection dbcon){
        this.error = "None";
        this.q = q;
        this.dbcon = dbcon;
    }
  
    @Override
    public void run() 
    {
        try{
            result = dbcon.createStatement().executeQuery(q);
        } catch(SQLSyntaxErrorException ex){
            error = "SQLSyntaxErrorException";
        }
        catch(SQLException ex)
        {
            error = "SQLException";
        }
    }
}

class UpdateQuery extends Thread  {
    public int rows;
    public String q;
    public Connection dbcon;
    public String error;
    UpdateQuery(String q, Connection dbcon){
        this.error = "None";
        this.q = q;
        this.dbcon = dbcon;
    }
  
    @Override
    public void run() 
    {
        try{
            rows = dbcon.createStatement().executeUpdate(q);
        } catch(SQLSyntaxErrorException ex){
            error = "SQLSyntaxErrorException";
        }
        catch(SQLException ex)
        {
            error = "SQLException";
        }
    }
}