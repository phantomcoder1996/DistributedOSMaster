package com.company;

import java.io.Serializable;
import java.util.ArrayList;

public class ClientMessage implements Serializable{

    public int tag;

    public ArrayList<String> content=new ArrayList<>();

}
