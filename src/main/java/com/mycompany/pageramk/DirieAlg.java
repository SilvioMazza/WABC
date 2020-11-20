package com.mycompany.pageramk;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Iterator;
import java.util.LinkedList;
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
import org.apache.hadoop.io.BooleanWritable;
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
import org.apache.hama.commons.util.TextPair;
import org.apache.hama.graph.AverageAggregator;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
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
public class DirieAlg {

    static int NUM_ITERATION = -1;

    public static class DirieVertex extends
            Vertex<Text, Text, CustomMapWritable> {

        private static Text master = new Text("-1");
        private static int AP_INDEX = 1;
        private static int RANK_INDEX = 2;
        private static int AGGREGATOR_INDEX = 4;
        private static int MESSAGE_TYPE = 5;
        private static int SYSTEM_STATUS = 6;
        private static int ERROR_INDEX = 7;
        private static int SENDER = 8;
        private static int CHECK_AGGREGATOR = 9;
        private static int OLD_RANK = 10;
        private static int SEED_MAP = 11;
        private static int SINGLE_SEED_DISTANCE = 12;
        private static int DISTANCE_SEED_SENDER = 13;
        private static int DISTANCE_FROM_SEED_MAP = 14;
        //MESSAGE TYPE
        private static int RANK_START = 1;
        private static int RANK_RECEIVED = 2;
        private static int WAITING_MASTER = 3;
        private static int DISTANCE_MEASURE = 4;
        ///STATUS TYPE
        private static int RANK_PROP = 1;
        private static int WAIT_MAX_FROM_MASTER = 2;
        private static int WAITING_TO_MAX = 3;
        private static int MEASURING_DISTANCE = 4;
        
        //PARAMETERS
        private static int k=15;

        @Override
        public void setup(HamaConfiguration conf) {
            if (!isMaster()) {
              
                this.getValue().put(new IntWritable(AP_INDEX), new DoubleWritable(0));
                this.getValue().put(new IntWritable(RANK_INDEX), new DoubleWritable(1));
                this.getValue().put(new IntWritable(MESSAGE_TYPE), new IntWritable(RANK_PROP));
                this.getValue().put(new IntWritable(SYSTEM_STATUS), new IntWritable(RANK_PROP));
                this.getValue().put(new IntWritable(SENDER), new IntWritable(Integer.parseInt(this.getVertexID().toString())));
                this.getValue().put(new IntWritable(CHECK_AGGREGATOR), new IntWritable(0));
                this.getValue().put(new IntWritable(SINGLE_SEED_DISTANCE), new DoubleWritable(Double.MAX_VALUE));
                this.getValue().put(new IntWritable(DISTANCE_FROM_SEED_MAP), new CustomMapWritable());
            } else {
                this.getValue().put(new IntWritable(MESSAGE_TYPE), new IntWritable(-1));
                this.getValue().put(new IntWritable(SYSTEM_STATUS), new IntWritable(-1));
                this.getValue().put(new IntWritable(SEED_MAP), new CustomMapWritable());
            }
        }

        @Override
        public void compute(Iterable<CustomMapWritable> messages) throws IOException {
            //    System.out.println(this.getVertexID().toString()+" "+this.getSuperstepCount());
            if (!isMaster()) {

                int systatus = ((IntWritable) getFromValue(this.getValue(), SYSTEM_STATUS)).get();
                // STATO CHE RAPPRESENTA IL MECCANISMO DI RANKING, QUANDO RICOMINCIA DEVO SETTARLO
                if (systatus == RANK_PROP) {
                    double mineValue = 0;
                    for (CustomMapWritable message : messages) {
                        double added_rank = ((DoubleWritable) message.get(new IntWritable(RANK_INDEX))).get();
                        int sender = ((IntWritable) message.get(new IntWritable(SENDER))).get();

                        double prob = findProb(sender);
                        if (Integer.parseInt(this.getVertexID().toString()) == sender) {
                            mineValue += 0;
                        } else {
                            mineValue += (prob) * added_rank;
                        }

                    }
                    double AP = ((DoubleWritable) getFromValue(this.getValue(), AP_INDEX)).get();

                    double final_value = (1 - AP) * (1 + mineValue);
                    double old_value = ((DoubleWritable) this.getValue().get(new IntWritable(RANK_INDEX))).get();
                    this.getValue().put(new IntWritable(OLD_RANK), new DoubleWritable(old_value));
                    int super_stepStart = ((IntWritable) getFromValue(this.getValue(), CHECK_AGGREGATOR)).get();
                //    System.out.println("SUPSTART: "+super_stepStart+"FINAL VALUE: "+final_value+" THIS IS: " + this.getVertexID().toString()+" SUPER: " + this.getSuperstepCount()+" AP: " + AP +" MINE: " + mineValue);

                    CustomMapWritable aggregated = ((CustomMapWritable) this.getAggregatedValue(0));
                    int counter = -1;
                    if (aggregated != null) {
                        counter = ((IntWritable) aggregated.get(new IntWritable(AGGREGATOR_INDEX))).get();
                       if(this.getVertexID().toString().equals("1"))
                        System.out.println("COUNTER: " + counter +" TOT"+this.getTotalNumVertices() +" DIFF: "+ (this.getSuperstepCount()-super_stepStart)+" "+((Double.parseDouble(String.valueOf(counter)))/Double.parseDouble(String.valueOf(this.getTotalNumVertices()))));
                    }
                    if (counter == this.getTotalNumVertices() - 1 || this.getSuperstepCount()-super_stepStart >=100|| ((Double.parseDouble(String.valueOf(counter)))/Double.parseDouble(String.valueOf(this.getTotalNumVertices())))>=0.98) {
                        CustomMapWritable agg = new CustomMapWritable();
                        agg.put(new IntWritable(AGGREGATOR_INDEX), new IntWritable(1));
                        this.aggregate(0, agg);
                        this.getValue().put(new IntWritable(SYSTEM_STATUS), new IntWritable(-1));
                        sendRankToMaster();
                    } else {
                        this.getValue().put(new IntWritable(RANK_INDEX), new DoubleWritable(final_value));
                        sendToNeighInverse();

                        if (this.getSuperstepCount() - super_stepStart > 2) {
                            if (Math.abs(old_value - final_value) < 0.001) {
                                CustomMapWritable cmp = new CustomMapWritable();
                                cmp.put(new IntWritable(AGGREGATOR_INDEX), new IntWritable(1));
                                this.aggregate(0, cmp);
                            }
                        }
                    }
                } else {
                    for (CustomMapWritable message : messages) {
                        int type = getMessageType(message);
                        if (type == WAITING_MASTER) {
                            this.getValue().put(new IntWritable(SINGLE_SEED_DISTANCE), new DoubleWritable(0));
                            this.getValue().put(new IntWritable(DISTANCE_SEED_SENDER), new IntWritable(Integer.parseInt(this.getVertexID().toString())));
                            this.getValue().put(new IntWritable(AP_INDEX), new DoubleWritable(1));
                            CustomMapWritable cmp = new CustomMapWritable();
                            cmp.put(new IntWritable(AGGREGATOR_INDEX), new IntWritable(1));
                             CustomMapWritable x= ((CustomMapWritable)getFromValue(this.getValue(), DISTANCE_FROM_SEED_MAP));
                                x.put(getFromValue(this.getValue(),DISTANCE_SEED_SENDER), new DoubleWritable(0));
                                
                            this.aggregate(0, cmp);
                            startMinDistance();
                        }
                        if (type == DISTANCE_MEASURE) {
                            double minDist = ((DoubleWritable) getFromValue(this.getValue(), SINGLE_SEED_DISTANCE)).get();
                            double mex_dist_val = ((DoubleWritable) getFromValue(message, SINGLE_SEED_DISTANCE)).get();
                            if (mex_dist_val < minDist) {
                                CustomMapWritable agg = new CustomMapWritable();
                                agg.put(new IntWritable(AGGREGATOR_INDEX), new IntWritable(1));
                                this.aggregate(0, agg);
                                this.getValue().put(new IntWritable(DISTANCE_SEED_SENDER), getFromValue(message,DISTANCE_SEED_SENDER));
                                CustomMapWritable x= ((CustomMapWritable)getFromValue(this.getValue(), DISTANCE_FROM_SEED_MAP));
                                x.put(getFromValue(message,DISTANCE_SEED_SENDER), new DoubleWritable(mex_dist_val));
                              
                                this.getValue().put(new IntWritable(DISTANCE_SEED_SENDER), getFromValue(message, DISTANCE_SEED_SENDER));
                                this.getValue().put(new IntWritable(SINGLE_SEED_DISTANCE), new DoubleWritable(mex_dist_val));
                                ///// FINO A QUI CORRETTO
                              //  ++++++++++++++++++++
                                sendDistance();
                            }
                        }
                    }

                    int aggregatedValue = ((IntWritable) ((CustomMapWritable) this.getAggregatedValue(0)).get(new IntWritable(AGGREGATOR_INDEX))).get();
                   if(this.getVertexID().toString().equals("1")){
                    System.out.println("DISTANCE COMP: " + aggregatedValue);}
                    //   System.out.println(aggregatedValue+" "+this.getSuperstepCount());
                    if (aggregatedValue == 0) {
                        
                        restartToRank();
                    }
                    if(aggregatedValue==-10){
                        voteToHalt();
                    }

                }

///MASTER MASTER MASTER MASTER MASTER MASTER MASTER
            } else {
                int maximum_vertex = -1;
                double maximum_rank = Double.MIN_VALUE;
                int message_number = 0;
                for (CustomMapWritable message : messages) {
                    message_number += 1;
                    int type = ((IntWritable) getFromValue(message, MESSAGE_TYPE)).get();
                    if (type == WAITING_MASTER) {

                        int vertex = ((IntWritable) getFromValue(message, SENDER)).get();
                        double rank_received = ((DoubleWritable) getFromValue(message, RANK_INDEX)).get();

                        if (rank_received > maximum_rank) {
                            maximum_vertex = vertex;
                            maximum_rank = rank_received;
                        }

                    }
                    if (message_number == this.getTotalNumVertices() - 1) {
                        this.getValue().put(new IntWritable(SYSTEM_STATUS), new IntWritable(WAITING_TO_MAX));
                    }
                }
                System.out.println(maximum_vertex+" ***");
                if (((IntWritable) getFromValue(this.getValue(), SYSTEM_STATUS)).get() == WAITING_TO_MAX) {
                    ((CustomMapWritable) getFromValue(this.getValue(), SEED_MAP)).put(new IntWritable(maximum_vertex), new BooleanWritable(true));
                    CustomMapWritable sMap=((CustomMapWritable)getFromValue(this.getValue(), SEED_MAP));
                    if(sMap.size()>=k){
                        sendHalt();
                        voteToHalt();
                    }else{
                    CustomMapWritable cmp = new CustomMapWritable();
                    cmp.put(new IntWritable(MESSAGE_TYPE), new IntWritable(WAITING_MASTER));
                    cmp.put(new IntWritable(AGGREGATOR_INDEX), new IntWritable(1));
                    this.aggregate(0, cmp);
                    sendMessage(new Text(String.valueOf(maximum_vertex)), cmp);
                    System.out.println("MASTER SEND START DISTANCE TO: " + maximum_vertex+" WITH RANK: " + maximum_rank);
                    this.getValue().put(new IntWritable(SYSTEM_STATUS), new IntWritable(-1));
                   
                }}
            }

        }
        
        public void sendHalt() throws IOException{
            CustomMapWritable cmp=new CustomMapWritable();
            cmp.put(new IntWritable(AGGREGATOR_INDEX), new IntWritable(-10));
            aggregate(0, cmp);
            System.out.println("*************************");
             CustomMapWritable seed_list= ((CustomMapWritable)getFromValue(this.getValue(), SEED_MAP));
                    System.out.println("PRINTING SEED LIST");
                    for(Writable i: seed_list.keySet()){
                        System.out.println(i);
                    }
                    System.out.println("SSTEP: " + this.getSuperstepCount());
        }

        public void restartToRank() {
            
            this.getValue().put(new IntWritable(SYSTEM_STATUS), new IntWritable(RANK_PROP));
            this.getValue().put(new IntWritable(RANK_INDEX), new DoubleWritable(0));
            this.getValue().put(new IntWritable(OLD_RANK), new DoubleWritable(0));
            CustomMapWritable seed_dist = ((CustomMapWritable) getFromValue(this.getValue(), DISTANCE_FROM_SEED_MAP));
            double total_distance = 0;
            double log_distance=0;
            for (Writable key : seed_dist.keySet()) {
                log_distance += Math.pow(10,-((DoubleWritable) seed_dist.get(key)).get());

            }
            total_distance=log_distance;
            if(total_distance>1){
                total_distance=1;
            }
            this.getValue().put(new IntWritable(CHECK_AGGREGATOR), new IntWritable(Integer.parseInt(String.valueOf(this.getSuperstepCount() + 1))));
            this.getValue().put(new IntWritable(AP_INDEX), new DoubleWritable(total_distance));
            this.getValue().put(new IntWritable(SINGLE_SEED_DISTANCE), new DoubleWritable(Double.MAX_VALUE));
        }

        public void sendDistance() throws IOException {
            for (Edge e : this.getEdges()) {
                TextPair val = (TextPair) e.getValue();
                int edge_type=Integer.parseInt(val.getSecond().toString());
                if(edge_type!=-1){
                double dist = Double.parseDouble(val.getFirst().toString());
                double actual_dist = ((DoubleWritable) getFromValue(this.getValue(), SINGLE_SEED_DISTANCE)).get();

                double send_value = actual_dist + (-1 * Math.log10(dist));
                CustomMapWritable message = new CustomMapWritable();
                message.put(new IntWritable(MESSAGE_TYPE), new IntWritable(DISTANCE_MEASURE));
                message.put(new IntWritable(SENDER), getFromValue(this.getValue(), DISTANCE_SEED_SENDER));
                message.put(new IntWritable(SINGLE_SEED_DISTANCE), new DoubleWritable(send_value));
                message.put(new IntWritable(DISTANCE_SEED_SENDER), getFromValue(this.getValue(), DISTANCE_SEED_SENDER));
                this.sendMessage(e, message);
                }
                }
        }

        public void startMinDistance() throws IOException {
            for (Edge e : this.getEdges()) {
                TextPair val = (TextPair) e.getValue();
                int real_edge=Integer.parseInt(val.getSecond().toString());
                if(real_edge!=-1){
                double dist = Double.parseDouble(val.getFirst().toString());
              
                double send_value = +(-1 * Math.log10(dist));
                CustomMapWritable message = new CustomMapWritable();
                message.put(new IntWritable(MESSAGE_TYPE), new IntWritable(DISTANCE_MEASURE));
                message.put(new IntWritable(DISTANCE_SEED_SENDER), getFromValue(this.getValue(), DISTANCE_SEED_SENDER));
                message.put(new IntWritable(SINGLE_SEED_DISTANCE), new DoubleWritable(send_value));
                this.sendMessage(e, message);
                }
                }

        }

        public void sendRankToMaster() throws IOException {
            CustomMapWritable message = new CustomMapWritable();
            message.put(new IntWritable(MESSAGE_TYPE), new IntWritable(WAITING_MASTER));
            message.put(new IntWritable(SENDER), new IntWritable(Integer.parseInt(this.getVertexID().toString())));
            message.put(new IntWritable(RANK_INDEX), getFromValue(this.getValue(), RANK_INDEX));
            this.sendMessage(master, message);

        }

        public double findProb(int sender) {
            for (Edge e : this.getEdges()) {
                if (Integer.parseInt(e.getDestinationVertexID().toString()) == sender) {
                    TextPair val = (TextPair) e.getValue();
                    double ret = Double.parseDouble(val.getFirst().toString());
                    return ret;
                }
            }
            if (Integer.parseInt(this.getVertexID().toString()) == sender) {
                return -1;
            }
            throw new NullPointerException();
        }

        public void sendToNeighInverse() throws IOException {
            for (Edge e : this.getEdges()) {
                TextPair val = (TextPair) e.getValue();
                int secondValue = Integer.parseInt(val.getSecond().toString());
                if (secondValue == -1) {
                    CustomMapWritable mess = new CustomMapWritable();
                    mess.put(new IntWritable(RANK_INDEX), ((DoubleWritable) getFromValue(this.getValue(), RANK_INDEX)));
                    mess.put(new IntWritable(SENDER), new IntWritable(Integer.parseInt(this.getVertexID().toString())));
                    mess.put(new IntWritable(MESSAGE_TYPE), new IntWritable(RANK_PROP));
                    sendMessage(e, mess);

                }
            }
        }

        public Writable getFromValue(CustomMapWritable source, int index) {
            Writable ret = ((Writable) ((CustomMapWritable) source).get(new IntWritable(index)));
            return ret;
        }

        public int getMessageType(CustomMapWritable message) {
            int messageType = ((IntWritable) message.get(new IntWritable(MESSAGE_TYPE))).get();
            return messageType;
        }

        public boolean isMaster() {
            return this.getVertexID().toString().equals(master.toString());
        }
    }

    public static class PagerankTextReader extends
            VertexInputReader<LongWritable, Text, Text, Text, DoubleWritable> {

        @Override
        public boolean parseVertex(LongWritable key, Text value,
                Vertex<Text, Text, DoubleWritable> vertex) throws Exception {

            String[] tokenArray = value.toString().split("\t");
            String vtx = tokenArray[11].trim();
            String[] edges = tokenArray[1].trim().split(" ");

            vertex.setVertexID(new Text(vtx));

            for (String v : edges) {
                vertex.addEdge(new Edge<Text, Text>(new Text(v), null));
            }

            return true;
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

    public static class DominatingPagerankJsonReader extends
            VertexInputReader<LongWritable, Text, Text, TextPair, CustomMapWritable> {

        @SuppressWarnings("unchecked")
        @Override
        public boolean parseVertex(LongWritable key, Text value,
                Vertex<Text, TextPair, CustomMapWritable> vertex) throws Exception {
            JSONArray jsonArray = (JSONArray) new JSONParser().parse(value.toString());
            vertex.setVertexID(new Text(jsonArray.get(0).toString()));

            String val = jsonArray.get(1).toString();
            vertex.setValue(new CustomMapWritable());
            Iterator<JSONArray> iter = ((JSONArray) jsonArray.get(2)).iterator();
            while (iter.hasNext()) {
                JSONArray edge = (JSONArray) iter.next();
                TextPair p = new TextPair();
                p.setFirst(new Text(edge.get(1).toString()));
                p.setSecond(new Text(edge.get(2).toString()));
                //System.out.println(edge.get(1).toString()+" "+edge.get(2).toString()+" "+edge.get(0).toString());
                vertex.addEdge(new Edge<Text, TextPair>(new Text(edge.get(0)
                        .toString()), p));
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

        GraphJob pageJob = new GraphJob(conf, DirieAlg.class);
        pageJob.setJobName("DominatingSet");

        pageJob.setVertexClass(DirieVertex.class);
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
        pageJob.setAggregatorClass(counterAggregator.class);

        // Vertex reader
        // According to file type, which is Text or Json,
        // Vertex reader handle it differently.
        if (cliParser.hasOption("f")) {
            if (cliParser.getOptionValue("f").equals("text")) {
                pageJob.setVertexInputReaderClass(DominatingTextReader.class);
            } else if (cliParser.getOptionValue("f").equals("json")) {
                pageJob.setVertexInputReaderClass(DominatingPagerankJsonReader.class);
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
        pageJob.setEdgeValueClass(TextPair.class);

        pageJob.setInputFormat(TextInputFormat.class);
        pageJob.setInputKeyClass(LongWritable.class);
        pageJob.setInputValueClass(Text.class);

        pageJob.setPartitioner(HashPartitioner.class);
        pageJob.setOutputFormat(TextOutputFormat.class);
        pageJob.setOutputKeyClass(Text.class);
        pageJob.setOutputValueClass(IntWritable.class);
        System.out.println(pageJob.getNumBspTask() + " TASK ATTIVI IC");
        System.out.println("RUNNING");
        return pageJob;
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, ParseException {
        System.out.println("CREATING MINE TASK");
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
