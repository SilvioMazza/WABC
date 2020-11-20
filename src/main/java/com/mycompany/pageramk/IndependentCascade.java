package com.mycompany.pageramk;

import static com.mycompany.pageramk.DominatingSet.DominatingSetVertex.STATUS_INDEX;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
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
public class IndependentCascade {

    static int NUM_ITERATION = -1;
    static int IS_SEED = 1;
    static int STATUS = 2;
    static int INFLUENCED_BY = 3;
    static int ACTIVATE = 4;
    static int RUNNING = 5;
    static int CASCADE_LENG=6;
    static int limit=1;
    static int threshold=0;

    public static class IndependentCascadeVertex extends Vertex<Text, Text, CustomMapWritable> {
        HashMap<Integer,Boolean>map=new HashMap<Integer,Boolean>();
        int[]vec={1811,1968,1702,1827,1322,1657,23,5645,89,3857,423,50,4053,19,7999};
        public void populate(){
            for(int i=0; i<15; i++){
                map.put(vec[i], true);
            }
        }
        @Override
        public void setup(HamaConfiguration conf) {
            populate();
      
            int seed= ((IntWritable)this.getValue().get(new IntWritable(IS_SEED))).get();
            seed=(map.containsKey(Integer.parseInt(this.getVertexID().toString())))?1:0;
            if(seed==1){
                System.out.println("aa: " + this.getVertexID().toString());
                this.getValue().put(new IntWritable(ACTIVATE),new IntWritable(1));
                this.getValue().put(new IntWritable(INFLUENCED_BY), new IntWritable(Integer.parseInt(((Text)this.getVertexID()).toString())));
                this.getValue().put(new IntWritable(CASCADE_LENG), new IntWritable(0));
            }
            else{
                this.getValue().put(new IntWritable(ACTIVATE),new IntWritable(0));
                this.getValue().put(new IntWritable(CASCADE_LENG), new IntWritable(0));
            }
            CustomMapWritable temp=new CustomMapWritable();
            temp.put(new IntWritable(ACTIVATE),new IntWritable(0));
            try {
                aggregate(0, temp);
            } catch (IOException ex) {
                Logger.getLogger(IndependentCascade.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        public void compute(Iterable<CustomMapWritable> messages) throws IOException {
            
            for (CustomMapWritable cmp : messages) {
                if (((IntWritable) getValue().get(new IntWritable(RUNNING))).get() == 1) {

                    int val = ((IntWritable) cmp.get(new IntWritable(ACTIVATE))).get();
                    int from_node = ((IntWritable) cmp.get(new IntWritable(INFLUENCED_BY))).get();
                    this.getValue().put(new IntWritable(INFLUENCED_BY), new IntWritable(from_node));
                    this.getValue().put(new IntWritable(ACTIVATE),new IntWritable(val));
                    int cascade_val=((IntWritable)cmp.get((new IntWritable(CASCADE_LENG)))).get();
                    this.getValue().put(new IntWritable(CASCADE_LENG), new IntWritable(cascade_val));
                    this.aggregate(0, this.getValue());
                    if (val == 1) {
                        
                        for (Edge e : this.getEdges()) {
                            int counter=0;
                            String v = ((Text) e.getValue()).toString();
                            double edge_value = Double.parseDouble(v);
                            int successfullTrial=0;
                            while(counter<limit){
                                
                            double sample = Math.random();
                           if(sample<=edge_value)
                               successfullTrial+=1;
                           counter++;
                            }
                            if (successfullTrial>threshold) {
                                CustomMapWritable cms = new CustomMapWritable();
                                cms.put(new IntWritable(ACTIVATE), new IntWritable(1));
                                cms.put(new IntWritable(INFLUENCED_BY), new IntWritable(((IntWritable)getValue().get(new IntWritable(INFLUENCED_BY))).get()));
                                cms.put(new IntWritable(CASCADE_LENG), new IntWritable(cascade_val+1));
                                this.sendMessage(e, cms);

                            }
                        }
                    this.getValue().put(new IntWritable(RUNNING), new IntWritable(0));
                    }

                }
            }

            if(getSuperstepCount()>0){
                CustomMapWritable ms= getAggregatedValue(0);
                int aggregationValue= ((IntWritable)ms.get(new IntWritable(ACTIVATE))).get();
                if( this.getVertexID().toString().equals("23"))
                System.out.println(aggregationValue);
                    if(aggregationValue==0){

                        
                        voteToHalt();
                    }
                }
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
            VertexInputReader<LongWritable, Text, Text, Text, CustomMapWritable> {

        @SuppressWarnings("unchecked")
        @Override
        public boolean parseVertex(LongWritable key, Text value,
                Vertex<Text, Text, CustomMapWritable> vertex) throws Exception {
            JSONArray jsonArray = (JSONArray) new JSONParser().parse(value.toString());
            CustomMapWritable cmp = new CustomMapWritable();
            vertex.setVertexID(new Text(jsonArray.get(0).toString()));
            String val = jsonArray.get(1).toString();
            cmp.put(new IntWritable(1), new IntWritable(Integer.parseInt(val)));
            cmp.put(new IntWritable(2), new IntWritable(0));
            cmp.put(new IntWritable(3), new IntWritable(-1));
            cmp.put(new IntWritable(5), new IntWritable(1));
            vertex.setValue(cmp);   
            Iterator<JSONArray> iter = ((JSONArray) jsonArray.get(2)).iterator();
            while (iter.hasNext()) {
                JSONArray edge = (JSONArray) iter.next();
                Edge edges = new Edge<Text, Text>(new Text(edge.get(0).toString()), new Text(edge.get(1).toString()));
                vertex.addEdge(new Edge<Text, Text>(new Text(edge.get(0)
                        .toString()), new Text(edge.get(1).toString())));

            }

            return true;
        }
    }

    public static GraphJob createJob(String[] args, HamaConfiguration conf,
            Options opts) throws IOException, ParseException {
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (!cliParser.hasOption("i") || !cliParser.hasOption("o")) {
            System.out
                    .println("No input or output path specified for IC, exiting.");
        }

        GraphJob pageJob = new GraphJob(conf, IndependentCascade.class);
        pageJob.setJobName("IndependentCascade");

        pageJob.setVertexClass(IndependentCascadeVertex.class);
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
        pageJob.setEdgeValueClass(Text.class);

        pageJob.setInputFormat(TextInputFormat.class);
        pageJob.setInputKeyClass(LongWritable.class);
        pageJob.setInputValueClass(Text.class);

        pageJob.setPartitioner(HashPartitioner.class);
        pageJob.setOutputFormat(TextOutputFormat.class);
        pageJob.setOutputKeyClass(Text.class);
        pageJob.setOutputValueClass(IntWritable.class);
        System.out.println(pageJob.getNumBspTask() + " TASK ATTIVI");
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
