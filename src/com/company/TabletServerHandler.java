package com.company;



import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

public class TabletServerHandler implements Runnable {

    Socket sid;
    Logger logger;

    public TabletServerHandler(Socket sid,Logger logger)
    {
        this.sid=sid;
        this.logger=logger;
    }


    void updateMeta()
    {
       //logger.logMsg("Tablet server with IP "+sid.getInetAddress()+" has requested updating meta data");

    }

    void updateDataBase()
    {

        //logger.logMsg("Tablet server with IP "+sid.getInetAddress()+" has requested updating DataBase");
    }

    @Override
    public void run() {


       try {
         //   while (sid==null);
            while(true) {
              //  System.out.println(sid.getInetAddress().toString());
                ObjectInputStream is = new ObjectInputStream(sid.getInputStream());

                while (true) {
                    Message msg = (Message) is.readObject();

                    int tag = msg.tag;

                    switch (tag) {
                        case 1: //update Meta
                        {

                        }

                        case 2://update database
                        {

                        }
                        case 3://log msg
                        {

                        //    System.out.println(msg.content);
                        }
                    }

                }
            }

        }
        catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
