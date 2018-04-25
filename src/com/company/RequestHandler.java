package com.company;


import com.sun.tools.javac.util.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import sun.plugin2.message.Message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class RequestHandler implements Runnable{

    Socket sid;
    Logger logger;

    RequestHandler(Socket clientSocket,Logger logger)
    {
        this.sid=clientSocket;
        this.logger=logger;
    }

    public void findRowKey()
    {




    }









    //--------------------------------------------Client request handlers---------------------------------------------


    @Override
    public void run() {


        try {
            ObjectInputStream is=new ObjectInputStream(sid.getInputStream());


                ClientMessage msg = (ClientMessage) is.readObject();

              //  System.out.println("Message from client "+msg.content.get(0));


              //  String[] ips = new String[msg.rowKeys.size()];

                  ArrayList<String> ips=new ArrayList<>();
                for (int i = 0; i < msg.content.size(); i++) {
                    String rowKey = msg.content.get(i);
                 //   System.out.println(rowKey +"received");
                    for (Pair p : Metadata.tabletIps.keySet()) {
                        String start = (String) p.fst;
                        String end = (String) p.snd;
                      //  System.out.println(start+" "+end+"\n\n");
                        int a = rowKey.compareTo(start);
                        int b = rowKey.compareTo(end);
                      //  System.out.println("a = "+a+" , b = "+b);
                        boolean isIntabletServer = ((a == 0) || (a > 0)) && ((b == 0) || (b < 0));
                        if (isIntabletServer) {
                            //System.out.println(Metadata.tabletIps);
                            ips.add(Metadata.tabletIps.get(p)); //get the ip of the server holding the meta data
                            break;
                        }

                    }
                }


                logger.logMsg("Client with IP "+sid.getInetAddress()+" has requested "+ msg.content.toString()+"\n");

            com.company.ClientMessage masterMsg=new com.company.ClientMessage();

                masterMsg.tag=1;



                masterMsg.content=ips;

            ObjectOutputStream os=new ObjectOutputStream(sid.getOutputStream());



            os.writeObject(masterMsg);







        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
