package com.company;

import com.sun.tools.javac.util.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class Metadata {


    public static JSONArray array = new JSONArray();

    public static Map<Pair<String,String>,String> tabletIps=new HashMap<>();

    public static int lastAssigned=0;

    public static int dataSize;

    public static int tabletSize;

    public static String tablet1="localhost";
    public static String tablet2="localhost";

    public static void initMaster()
    {

        try
        {
            // create our mysql database connection
            String myDriver = "org.gjt.mm.mysql.Driver";
            String myUrl = "jdbc:mysql://localhost/youtube2";
            Class.forName(myDriver);
            Connection conn = DriverManager.getConnection(myUrl, "root", "");

            // our SQL SELECT query.
            // if you only need a few columns, specify them by name instead of using "*"
            String query = "SELECT * FROM `table1` ";

            // create the java statement
            Statement st = conn.createStatement();

            // execute the query, and get a java resultset
            ResultSet rs = st.executeQuery(query);

            // iterate through the java resultset
            while (rs.next())
            {
                String id = rs.getString("rowKey");
                String title = rs.getString("title");
                String tags = rs.getString("tags");
                int views = rs.getInt("views");
                int likes = rs.getInt("likes");
                int dislikes= rs.getInt("dislikes");
                int comment_count=rs.getInt("comment_count");
                String description=rs.getString("description");



                JSONObject item = new JSONObject();
                item.put("rowKey", id);
                item.put("title", title);
                item.put("tags", tags);
                item.put("views",views);
                item.put("likes",likes);
                item.put("dislikes",dislikes);
                item.put("comment_count",comment_count);
                item.put("description",description);


                array.add(item);

            }
            st.close();
        }
        catch (Exception e)
        {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }




    }



    public static void createMetaTable()
    {

         dataSize=array.size();
         tabletSize=dataSize/3;

        JSONObject range1S=(JSONObject)array.get(0);
        JSONObject range1E=(JSONObject)array.get(tabletSize);
        JSONObject range2S=(JSONObject)array.get(tabletSize+1);
        JSONObject range2E=(JSONObject)array.get(2*tabletSize);
        JSONObject range3S=(JSONObject)array.get(2*tabletSize+1);
        JSONObject range3E=(JSONObject)array.get(dataSize-1);

        Pair<String,String> shard1=new Pair(range1S.get("rowKey"),range1E.get("rowKey"));
        Pair<String,String> shard2=new Pair(range2S.get("rowKey"),range2E.get("rowKey"));
        Pair<String,String> shard3=new Pair(range3S.get("rowKey"),range3E.get("rowKey"));

        tabletIps.put(shard1,tablet1);
        tabletIps.put(shard2,tablet2);
        tabletIps.put(shard3,tablet2);

        System.out.println(tablet1);
        System.out.println(tablet2);


    }
}
