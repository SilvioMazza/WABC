package com.mycompany.pageramk;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import static com.mycompany.pageramk.IndependentCascade.INFLUENCED_BY;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

/**
 *
 * @author silvio
 */
public class CustomMapWritable extends MapWritable {
    static int IS_SEED = 0;
    static int RANK = 1;
    static int MASTER_SEED_LIST_INDEX = 2;
    static int NEIGH_SET_INDEX = 3;
    static int TYPE_MESSAGE = 4;
    static int SENDER_NODE = 5;
    static int NEIGH_LIST = 6;
    static int SEED_POINTER = 7;
    static int INFLUENCED_INDEX = 8;
    static int INFLUENCED_NODE=9;
    static int INFLUENCE_SOURCE=10;
    static int ACTIVATION_MESSAGE = 0;
    static int ACTIVATE = 1;
    static int UPDATE_INFLUENCE = 2;
    static int ACTIVATION_INFO = 3;

    @Override
    public String toString() {
       return String.valueOf(get(new IntWritable(4))) +"/"+ String.valueOf(get(new IntWritable(INFLUENCED_BY)));
    
  
  

//To change body of generated methods, choose Tools | Templates.
    }
    
    public void printInfo(){
        System.out.println("MT: "+this.get(new IntWritable(TYPE_MESSAGE))+" IN"+this.get(new IntWritable(INFLUENCED_NODE))+" IS"+this.get(new IntWritable(INFLUENCE_SOURCE)));
        
    }
    
    
}
