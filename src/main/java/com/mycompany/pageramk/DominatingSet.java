package com.mycompany.pageramk;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.AverageAggregator;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author silvio
 */
public class DominatingSet {

    static int NUM_ITERATION = -1;

    public static class DominatingSetVertex extends Vertex<Text, Text, CustomMapWritable> {

        final private int BECOME_BLACK = 1;
        final static int REST_WHITE = 0;
        final static int WHITE = 0;
        final static int BLACK = 1;
        final static int GREY = 2;
        final static int INACTIVE = 1;
        final static int ACTIVE=0;
        final static int STATUS_INDEX = 0;
        final static int SPAN_VALUE = 1;
        final static int SPAN_MAX = 2;
        final static int DECISION_INDEX = 3;
        final static int REAL_ARC=1;
        final static int NOT_REAL_ARC=0;
        final static int ARC_TYPE=4;
        final static int WHITE_NEIGH_INDEX = 5;
        final static int ACTIVE_INDEX=6;

        
        @Override
        public void setup(HamaConfiguration conf) {
          
            CustomMapWritable mw = new CustomMapWritable();
            mw.put(new IntWritable(STATUS_INDEX), new IntWritable(0));
            mw.put(new IntWritable(SPAN_MAX), new IntWritable(0));
            mw.put(new IntWritable(WHITE_NEIGH_INDEX), new IntWritable(0));
            mw.put(new IntWritable(SPAN_VALUE), new IntWritable(0));
            mw.put(new IntWritable(DECISION_INDEX), new IntWritable(0));
            mw.put(new IntWritable(ARC_TYPE), new IntWritable(0));
                        mw.put(new IntWritable(ACTIVE_INDEX), new IntWritable(0));

            setValue(mw);

            try {
                sendStatusToNeigh();
            } catch (IOException ex) {
                Logger.getLogger(DominatingSet.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        @Override
        public void compute(Iterable<CustomMapWritable> messages) throws IOException {

            if (getValueMap(ACTIVE_INDEX) == INACTIVE) {

            } else {
                if (this.getSuperstepCount() >= 1) {

                    if (this.getSuperstepCount() % 8 == 1) {
                        //RICEZIONE DELLO STATO DEI VICINI
                        for (CustomMapWritable msg : messages) {

                            if(((IntWritable)msg.get(new IntWritable(ARC_TYPE))).get()==NOT_REAL_ARC){

                            switch (((IntWritable) msg.get(new IntWritable(STATUS_INDEX))).get()) {
                                case (WHITE):
                                    putValueMap(WHITE_NEIGH_INDEX, getValueMap(WHITE_NEIGH_INDEX) + 1);
                            }
                            }
                        }

                    } else if (this.getSuperstepCount() % 8 == 2) {

                        //ACCUMULO INFORMAZIONI, CALCOLO SPAN E INVIO SPAN
                        if (getValueMap(WHITE_NEIGH_INDEX) == 0 && getValueMap(STATUS_INDEX) != WHITE) {
                            putValueMap(ACTIVE_INDEX, INACTIVE);

                            voteToHalt();
                           return;
                        } else {

                            putValueMap(SPAN_VALUE, getValueMap(SPAN_VALUE) + getValueMap(WHITE_NEIGH_INDEX));

                            if (getValueMap(STATUS_INDEX) == WHITE) {
                                putValueMap(SPAN_VALUE, getValueMap(SPAN_VALUE) + 1);
                            }


                            putValueMap(SPAN_MAX, 0);

                            sendSpanToNeighTTL2();
                        }

                    } else if (this.getSuperstepCount() % 8 == 3) {
                        //RACCOLTA SPAN RICEVUTI CALCOLO MASSIMO E INVIO
                        for (CustomMapWritable msg : messages) {

                            if (((IntWritable) msg.get(new IntWritable(SPAN_VALUE))).get() > getValueMap(SPAN_MAX)) {
                                putValueMap(SPAN_MAX, ((IntWritable) msg.get(new IntWritable(SPAN_VALUE))).get());
                            }

                        }

                    } else if (this.getSuperstepCount() % 8 == 4) {
                        //INOLTRO SPAN AGLI ALTRI
                        sendSpanToNeighTTL1();
                    } else if (this.getSuperstepCount() % 8 == 5) {
                        //RICEZIONE DELLO SPAN DAGLI ALTRI
                        for (CustomMapWritable msg : messages) {

                            if (((IntWritable) msg.get(new IntWritable(SPAN_VALUE))).get() > getValueMap(SPAN_MAX)) {

                                putValueMap(SPAN_MAX, ((IntWritable) msg.get(new IntWritable(SPAN_VALUE))).get());

                            }
                        }
                        CustomMapWritable toAggr=new CustomMapWritable();
                        toAggr.put(new IntWritable(100), new IntWritable(getValueMap(SPAN_MAX)));
                        this.aggregate(0, toAggr);

                    } else if (this.getSuperstepCount() % 8 == 6) {
                        int aggregator= ((IntWritable)this.getAggregatedValue(0).get(new IntWritable(100))).get();
                        if (getValueMap(SPAN_MAX) > getValueMap(SPAN_VALUE) || getValueMap(SPAN_VALUE)<aggregator) {
                            sendDecisionToNeigh(REST_WHITE);

                        } else {
                            sendDecisionToNeigh(BECOME_BLACK);
                            System.out.println("IM VERTEX: " + getVertexID() + " AND I BECOME BLACK" +" SS: " + this.getSuperstepCount());
                            putValueMap(STATUS_INDEX, BLACK);

                        }
                    } else if (this.getSuperstepCount() % 8 == 7) {
                        for (CustomMapWritable msg : messages) {
                            if(((IntWritable) msg.get(new IntWritable(ARC_TYPE))).get()==REAL_ARC){

                                if (((IntWritable) msg.get(new IntWritable(DECISION_INDEX))).get() == BECOME_BLACK && getValueMap(STATUS_INDEX)!=BLACK) {
                                putValueMap(STATUS_INDEX, GREY);

                            }

                        }
                        }
                    } else if (this.getSuperstepCount() % 8 == 0) {
                        putValueMap(WHITE_NEIGH_INDEX, 0);
                        putValueMap(SPAN_VALUE,0);
                        putValueMap(SPAN_MAX,0);
                        sendStatusToNeigh();
                    }
                }
            }
        }

       

     
        public void putValueMap(int index, int value) {
            getValue().put(new IntWritable(index), new IntWritable(value));
        }

        public int getValueMap(int index) {
            CustomMapWritable mw = getValue();
            IntWritable val = (IntWritable) mw.get(new IntWritable(index));
            return val.get();
        }

        public void sendDecisionToNeigh(int value) throws IOException {
//MODIFICA
 for (Edge e : getEdges()) {

                Text valueVertex = (Text) e.getValue();
                int valore = Integer.parseInt(valueVertex.toString());

                sendMessage(e, makeDoubleMessage(DECISION_INDEX, ARC_TYPE, value, valore==1?REAL_ARC:NOT_REAL_ARC));
            }
        }

        public void sendSpanToNeighTTL2() throws IOException {
            for (Edge e : getEdges()) {

                Text valueVertex = (Text) e.getValue();
                int valore = Integer.parseInt(valueVertex.toString());

                sendMessage(e, makeMessage(SPAN_VALUE, getValueMap(SPAN_VALUE) * valore));
            }
        }
        public void sendSpanToNeighTTL1Secondo()throws IOException{
             for (Edge e : getEdges()) {

                Text valueVertex = (Text) e.getValue();
                int valore = Integer.parseInt(valueVertex.toString());

                sendMessage(e, makeMessage(SPAN_VALUE, getValueMap(SPAN_MAX) * valore));
            }
        }
        public void sendSpanToNeighTTL1() throws IOException {

            sendMessageToNeighbors(makeMessage(SPAN_VALUE, getValueMap(SPAN_MAX)));
        }

        public void sendStatusToNeigh() throws IOException {
            for (Edge e: getEdges()){
                Text valueVertex=(Text)e.getValue();
                int valore= Integer.parseInt(valueVertex.toString());
                sendMessage(e, makeDoubleMessage(STATUS_INDEX, ARC_TYPE, getValueMap(STATUS_INDEX), valore==1? REAL_ARC:NOT_REAL_ARC));
            }
         //   sendMessageToNeighbors(makeMessage(STATUS_INDEX, getValueMap(STATUS_INDEX)));
        }
        public CustomMapWritable makeDoubleMessage(int index1, int index2, int value1, int value2){
             CustomMapWritable mw = new CustomMapWritable();

            mw.put(new IntWritable(STATUS_INDEX), new IntWritable(0));
            mw.put(new IntWritable(SPAN_MAX), new IntWritable(0));

            mw.put(new IntWritable(SPAN_VALUE), new IntWritable(0));
            mw.put(new IntWritable(ARC_TYPE), new IntWritable(0));
            mw.put(new IntWritable(DECISION_INDEX), new IntWritable(0));

            mw.put(new IntWritable(index1), new IntWritable(value1));
            mw.put(new IntWritable(index2), new IntWritable(value2));
            return mw;
        }
        public CustomMapWritable makeMessage(int index, int value) {
            CustomMapWritable mw = new CustomMapWritable();

            mw.put(new IntWritable(STATUS_INDEX), new IntWritable(0));
            mw.put(new IntWritable(SPAN_MAX), new IntWritable(0));
            mw.put(new IntWritable(ARC_TYPE), new IntWritable(0));

            mw.put(new IntWritable(SPAN_VALUE), new IntWritable(0));

            mw.put(new IntWritable(DECISION_INDEX), new IntWritable(0));

            mw.put(new IntWritable(index), new IntWritable(value));
            return mw;
        }
    }

    public static class DominatingTextReader extends
            VertexInputReader<LongWritable, Text, Text, NullWritable, ArrayWritable> {

        @Override
        public boolean parseVertex(LongWritable key, Text value,
                Vertex<Text, NullWritable, ArrayWritable> vertex) throws Exception {

            String[] tokenArray = value.toString().split("\t");
            String vtx = tokenArray[11].trim();
            String[] edges = tokenArray[1].trim().split(" ");

            vertex.setVertexID(new Text(vtx));

            for (String v : edges) {
                vertex.addEdge(new Edge<Text, NullWritable>(new Text(v), null));
            }

            return true;
        }
    }

    public static class DominatingJsonReader extends
            VertexInputReader<LongWritable, Text, Text, Text, CustomMapWritable> {

        @SuppressWarnings("unchecked")
        @Override
        public boolean parseVertex(LongWritable key, Text value,
                Vertex<Text, Text, CustomMapWritable> vertex) throws Exception {
            JSONArray jsonArray = (JSONArray) new JSONParser()
                    .parse(value.toString());

            vertex.setVertexID(new Text(jsonArray.get(0).toString()));
            CustomMapWritable mw=new CustomMapWritable();
            mw.put(new IntWritable(0), new IntWritable(0));
            mw.put(new IntWritable(1), new IntWritable(0));
            mw.put(new IntWritable(2), new IntWritable(0));
            mw.put(new IntWritable(3), new IntWritable(0));
            mw.put(new IntWritable(5), new IntWritable(0));
                        mw.put(new IntWritable(4), new IntWritable(0));

            vertex.setValue(mw);
            Iterator<JSONArray> iter = ((JSONArray) jsonArray.get(2)).iterator();
            while (iter.hasNext()) {
                JSONArray edge = (JSONArray) iter.next();
                Edge edges = new Edge<Text, Text>(new Text(edge.get(0).toString()), new Text(edge.get(1).toString()));
                String value2= edge.get(2).toString();
                int value2d= Integer.parseInt(value2);
                if(value2d==-1)
                    value2d=0;
                vertex.addEdge(new Edge<Text, Text>(new Text(edge.get(0)
                        .toString()), new Text(String.valueOf(value2d))));

            }

          

            return true;
        }
    }

    public static GraphJob createJob(String[] args, HamaConfiguration conf,
            Options opts) throws IOException, ParseException {
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (!cliParser.hasOption("i") || !cliParser.hasOption("o")) {
            System.out
                    .println("No input or output path specified for DominatingSet, exiting.");
        }

        GraphJob pageJob = new GraphJob(conf, DominatingSet.class);
        pageJob.setJobName("DominatingSet");

        pageJob.setVertexClass(DominatingSetVertex.class);
        pageJob.setInputPath(new Path(cliParser.getOptionValue("i")));
        pageJob.setOutputPath(new Path(cliParser.getOptionValue("o")));

        // set the defaults
        pageJob.setMaxIteration(NUM_ITERATION);
        // reference vertices to itself, because we don't have a dangling node
        // contribution here

        if (cliParser.hasOption("t")) {
            pageJob.setNumBspTask(Integer.parseInt(cliParser.getOptionValue("t")));
        }
        // error
        pageJob.setAggregatorClass(MaxAggr.class);

        // Vertex reader
        // According to file type, which is Text or Json,
        // Vertex reader handle it differently.
        if (cliParser.hasOption("f")) {
            if (cliParser.getOptionValue("f").equals("text")) {
                pageJob.setVertexInputReaderClass(DominatingTextReader.class);
            } else if (cliParser.getOptionValue("f").equals("json")) {
                pageJob.setVertexInputReaderClass(DominatingJsonReader.class);
            } else {
                System.out.println("File type is not available to run DominatingSet... "
                        + "File type set default value, Text.");
                pageJob.setVertexInputReaderClass(DominatingTextReader.class);
            }
        } else {
            pageJob.setVertexInputReaderClass(DominatingTextReader.class);
        }

        pageJob.setVertexIDClass(Text.class);
        pageJob.setVertexValueClass(CustomMapWritable.class);
        pageJob.setEdgeValueClass(Text.class);

        pageJob.setInputFormat(TextInputFormat.class);
        pageJob.setInputKeyClass(LongWritable.class);
        pageJob.setInputValueClass(CustomMapWritable.class);

        pageJob.setPartitioner(HashPartitioner.class);
        pageJob.setOutputFormat(TextOutputFormat.class);
        pageJob.setOutputKeyClass(Text.class);
        pageJob.setOutputValueClass(IntWritable.class);
        System.out.println(pageJob.getNumBspTask() + " TASK ATTIVI DOM RUNNING");
        return pageJob;
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, ParseException {

        Options opts = new Options();
        opts.addOption("i", "input_path", true, "The Location of output path.");
        opts.addOption("o", "output_path", true, "The Location of input path.");
        opts.addOption("h", "help", false, "Print usage");
        opts.addOption("t", "task_num", true, "The number of tasks.");
        opts.addOption("f", "file_type", true, "The file type of input data. Input"
                + "file format which is \"text\" tab delimiter separated or \"json\"."
                + "Default value - Text");

        if (args.length < 2) {
            new HelpFormatter().printHelp("pagerank -i INPUT_PATH -o OUTPUT_PATH "
                    + "[-t NUM_TASKS] [-f FILE_TYPE]", opts);
            System.exit(-1);
        }

        HamaConfiguration conf = new HamaConfiguration();
        GraphJob pageJob = createJob(args, conf, opts);

        long startTime = System.currentTimeMillis();
        if (pageJob.waitForCompletion(true)) {
            System.out.println("Job Finished in "
                    + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        }
    }
}
