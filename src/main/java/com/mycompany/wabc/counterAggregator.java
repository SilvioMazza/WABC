/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.wabc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hama.graph.AbstractAggregator;

/**
 *
 * @author silvio
 */
public class counterAggregator extends
    AbstractAggregator<CustomMapWritable> {
    int newAggregation=0;

    @Override
    public void aggregate(CustomMapWritable value) {
        newAggregation+=((IntWritable)value.get(new IntWritable(4))).get(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public CustomMapWritable finalizeAggregation() {
        CustomMapWritable cmp=new CustomMapWritable();
        cmp.put(new IntWritable(4), new IntWritable(newAggregation));
return cmp;//To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public CustomMapWritable getValue() {
          CustomMapWritable cmp=new CustomMapWritable();
        cmp.put(new IntWritable(4), new IntWritable(newAggregation));
return cmp;//To change body of generated methods, choose Tools | Templates. //To change body of generated methods, choose Tools | Templates.
    }
    
    

   
    
    
}
