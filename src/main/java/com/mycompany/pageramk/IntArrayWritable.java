/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.pageramk;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/**
 *
 * @author silvio
 */
public  class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() {
        super(IntWritable.class);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String s : super.toStrings())
        {
            sb.append(s).append(" ");
        }
        return sb.toString();
    }
}