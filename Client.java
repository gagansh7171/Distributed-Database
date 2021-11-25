// A Java program for a Client
import java.net.*;
import java.io.*;
import java.util.*;
import java.util.stream.*;
public class Client
{
    // initialize socket and input output streams
    private Socket socket            = null;
    private BufferedReader input   = null;
    private DataOutputStream out     = null;
    private DataInputStream in       =  null;

    private void printTabularFormat(String s){
        int count = 0;
        for(int i = 0; i<s.length();i++){
            if(s.charAt(i) == ';') count++;
        }
        String[] splitted = s.split(";");
        String[][] table = new String[count][];
        int itr = 0;
        for (String string : splitted) {
            if(string.length() > 0){
                table[itr] =  string.split(",");
            }
            itr++;
        }
        itr=0;
        Map<Integer, Integer> columnLengths = new HashMap<>();
        Arrays.stream(table).forEach(a -> Stream.iterate(0, (i -> i < a.length), (i -> ++i)).forEach(i -> {
            if (columnLengths.get(i) == null) {
                columnLengths.put(i, 0);
            }
            if (columnLengths.get(i) < a[i].length()) {
                columnLengths.put(i, a[i].length());
            }
        }));
        
        for(int i = 0; i<count; i++){
            for(int j = 1; j<table[i].length; j++){
                System.out.print(String.format("| %-"+columnLengths.get(j)+"s", table[i][j])+ " " );
            }
            System.out.println("|");
        }

    }
    // constructor to put ip address and port
    public Client(String address, int port)
    {
        // establish a connection
        try
        {
            socket = new Socket(address, port);
            System.out.println("Connected");
  
            // takes input from terminal
            input  = new BufferedReader(
                new InputStreamReader(System.in));
            // recieves data from socket
            in = new DataInputStream(
                new BufferedInputStream(socket.getInputStream()));
            // sends output to the socket
            out = new DataOutputStream(socket.getOutputStream());
        }
        catch(UnknownHostException u)
        {
            System.out.println(u);
        }
        catch(IOException i)
        {
            System.out.println(i);
        }
  
        // string to read message from input
        String line = "";
        
        System.out.println("Supported queries are SELECT, INSERT, UPDATE, DELETE, DESC and SHOW TABLES. Query 'OVER' for closing connection and 'OVER and OUT' for closing connection and shutting down server.");
        // keep reading until "Over" is input
        try {
            while (!line.strip().toUpperCase().equals("OVER") && !line.strip().toUpperCase().equals("OVER AND OUT"))
            {
                System.out.print("Query: ");
                line = input.readLine();
                if(line.strip().toUpperCase().equals("SHOW TABLES")){
                    printTabularFormat("ID,Tables;1,faculty;2,student;");
                }
                else{
                    out.writeUTF(line);
                    String readline = in.readUTF();
                    if(
                        (readline.length()>5 && readline.substring(0,5).equals("Error")) || 
                        (readline.length()>6 && readline.substring(0,6).equals("INSERT")) || 
                        (readline.length()>6 && readline.substring(0,6).equals("DELETE")) || 
                        (readline.length()>6 && readline.substring(0,6).equals("UPDATE"))
                    )
                        System.out.println(readline);
                    else
                        printTabularFormat(readline);
                }
            }
        }
        catch(EOFException ex)
        {
            System.out.println("Closing the Connection");
        }
        catch(IOException i)
        {
            System.out.println("Server Offline");
        }
  
        // close the connection
        try
        {
            input.close();
            out.close();
            in.close();
            socket.close();
        }
        catch(IOException i)
        {
            System.out.println("Server Offline");
        }
    }
  
    public static void main(String args[])
    {
        Client client = new Client("127.0.0.1", 5000);
    }
}