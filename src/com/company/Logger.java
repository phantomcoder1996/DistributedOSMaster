package com.company;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Logger {

    BufferedWriter writer;

    {
        try {
            writer = new BufferedWriter(new FileWriter("log.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void logMsg(String  msg)
    {
        try {
            writer.write(msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
