package com.company;


import com.sun.tools.javac.util.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Main {

  //define metadata for master



    public static Socket tablet1id;
    public static Socket tablet2id;



//    public static void WaitForIps()
//    {
//        int count=0;
//
//        while(count<2)
//        {
//
//
//        }
//    }

    public static void assignLoads()
    {

        try {

            JSONArray shard=new JSONArray();

             tablet1id=new Socket(InetAddress.getByName("localhost"),9090);
           //  tablet2id=new Socket(Metadata.tablet2,9090);

            for(int i=0;i<=Metadata.tabletSize;i++)
            {
                shard.add(Metadata.array.get(i));
            }

            com.company.Message msg= new Message();
            msg.midIndex=-1;

            msg.content=shard.toString();
            msg.tag=1;

            ObjectOutputStream os1=new ObjectOutputStream(tablet1id.getOutputStream());
           // ObjectOutputStream os2=new ObjectOutputStream(tablet2id.getOutputStream());

            os1.writeObject(msg);

            for(int i=Metadata.tabletSize+1;i<Metadata.dataSize;i++)
            {
                shard.add(Metadata.array.get(i));
            }
            msg= new Message();
            msg.midIndex=Metadata.tabletSize-1;

            msg.content=shard.toString();
            msg.tag=1;

          //  os2.writeObject(msg);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }





    public static void main(String[] args) {
	// write your code here

        Metadata.initMaster();
        Metadata.createMetaTable();
        assignLoads();
        Logger logger=new Logger();


        TabletServerHandler ts1=new TabletServerHandler(tablet1id,logger);
        Thread tablet1Thread=new Thread(ts1);
        tablet1Thread.start();
       // TabletServerHandler ts2=new TabletServerHandler(tablet2id,logger);
       // Thread tablet2Thread=new Thread(ts2);
      //    tablet2Thread.start();



        try {
            ServerSocket masterListener=new ServerSocket(9000);

            while(true) {
               Socket clientSocket= masterListener.accept();
               RequestHandler rq=new RequestHandler(clientSocket,logger);
               Thread reqHandler=new Thread(rq);
              reqHandler.start();


            }


        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
