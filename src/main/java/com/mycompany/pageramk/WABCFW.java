package com.mycompany.pageramk;


import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.commons.util.TextPair;
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
public class WABCFW {
    //max number of superstep for the graph job
    static int NUM_ITERATION = -1;
    static Text master = new Text("-1");
    // field that establishes if a node is initially selected as seed
    static int IS_SEED = 0;
    // field to get the rank of a node
    static int RANK = 1;
    // fielt to get the seed list
    static int MASTER_SEED_LIST_INDEX = 2;
    // field to get the neighb.
    static int NEIGH_SET_INDEX = 3;
    // field to get the type of the message
    static int TYPE_MESSAGE = 4;
    // field to get the sender of a message
    static int SENDER_NODE = 5;
    // field to send dei list of neigh.
    static int NEIGH_LIST = 6;
    // fielt to get the seed list of the master
    static int SEED_POINTER = 7;
    // field to get the influenced nodes
    static int INFLUENCED_INDEX = 8;
    // field to signal if a node is influenced
    static int INFLUENCED_NODE = 9;
    // field to get the influence source
    static int INFLUENCE_SOURCE = 10;
    // field to get ranking
    static int RANKING_INDEX = 11;
    // field of neigh for submap
    static int NEIGH_ACCESS_LIST = 12;
    // field to know if the propagation is running
    static int IS_RUNNING_PROPAGATION = 13;
    // field to get the switch SB - EB map
    static int SWITCH_NODE_INFLUENCE = 14;
    // field to get the switched node
    static int SWITCH_NAME_NODE = 15;
    // field to establishes if there's a switch
    static int SWITCH_EVAL = 16;
    // field to establishes if there's convergence
    static int CONV_SWITCH = 17;
    // fieldo to get maximum distance
    static int DISTANCE_INDEX = 18;
    // field to know if a node has already know the propagation strength of source node
    static int PROPAGATION_DONE = 19;
    // field to know if boot is completed
    static int ALL_SEED_SELECTED = 20;
    // field to get the maximum number of neigh.
    static int NEIGH_NUMBER = 21;
    // field to get the probability of influence
    static int PROBABILITY_INDEX=22;
    // field to get the previous convergence value
    static int OLD_VAL=23;
    
    
    
    
    
    
    
    // cutting parameter for the number of neigh that will be evaluated
    static int MAX_NEIGH = Integer.MAX_VALUE;

    //index value
    static int ACTIVATION_MESSAGE = 0;
    static int ACTIVATE = 1;
    static int UPDATE_INFLUENCE = 2;
    static int ACTIVATION_INFO = 3;
    static int UPDATE_DATA = 4;
   
    // if false the neigh list is not sorted on the basis of ranking
    static boolean topForNeigh=false;
    // establish the number of neigh. to be evaluated
    static int number_of_top=Integer.MAX_VALUE;
    // if distance is > 0 the influence chains is breaked after $distance step
    static int DISTANCE = -1;
    // number of seed 
    static int k = 15;
    //Omega value
    static double STOPPING_PERC_VALUE = 0.02;
    // if false the fitness eval has no weight, a seed that influences another node has +1 in F.Eval
    static boolean weightedAlgorithm=true;
    
    //number of trial for a node to influence a neigh.
    private static int limit=1;
    //thershold that indicates the number of positive attempt to influence a node (limite must be >= threshold) if -1 a node always influence another node
    // note this parameter change the fitness evaluation of eache node, you can introduce a non deterministic source
    private static int threshold=-1;
    
    

    public static class WABC extends Vertex<Text, TextPair, CustomMapWritable> {

        @Override
        public void setup(HamaConfiguration conf) {
            if (isMaster()) {
                IntArrayWritable aw = new IntArrayWritable();
                CustomMapWritable mapCover = new CustomMapWritable();
                IntWritable[] seed_list = new IntWritable[k];
                for (int i = 0; i < seed_list.length; i++) {
                    seed_list[i] = new IntWritable(-1);
                }
                CustomMapWritable influenceMap = new CustomMapWritable();
                aw.set(seed_list);
                this.getValue().put(new IntWritable(MASTER_SEED_LIST_INDEX), aw);
                this.getValue().put(new IntWritable(NEIGH_SET_INDEX), mapCover);
                this.getValue().put(new IntWritable(SEED_POINTER), new IntWritable(0));
                this.getValue().put(new IntWritable(INFLUENCED_INDEX), influenceMap);
                this.getValue().put(new IntWritable(TYPE_MESSAGE), new IntWritable(1));
                this.getValue().put(new IntWritable(IS_RUNNING_PROPAGATION), new BooleanWritable(false));
                this.getValue().put(new IntWritable(SWITCH_NAME_NODE), new IntWritable(-1));
                this.getValue().put(new IntWritable(SWITCH_NODE_INFLUENCE), new CustomMapWritable());
                this.getValue().put(new IntWritable(SWITCH_EVAL), new BooleanWritable(false));
                this.getValue().put(new IntWritable(CONV_SWITCH), new BooleanWritable(false));
                this.getValue().put(new IntWritable(ALL_SEED_SELECTED), new BooleanWritable(false));
                this.getValue().put(new IntWritable(NEIGH_NUMBER), new IntWritable(0));
                this.getValue().put(new IntWritable(OLD_VAL), new DoubleWritable(0));

            } else {
                int seed_node = ((IntWritable) getFromMap(this.getValue(), IS_SEED)).get();
                if (seed_node == 1) {
                    this.getValue().put(new IntWritable(TYPE_MESSAGE), new IntWritable(ACTIVATION_MESSAGE));

                } else {
                    this.getValue().put(new IntWritable(TYPE_MESSAGE), new IntWritable(-1));
                }

                CustomMapWritable mp = new CustomMapWritable();
                this.getValue().put(new IntWritable(PROPAGATION_DONE), mp);
            }
        }

        @Override
        public void compute(Iterable<CustomMapWritable> messages) throws IOException {

            if (!isMaster()) {
               
                if (this.getSuperstepCount() > 2) {
                    CustomMapWritable agg = getAggregatedValue(0);
                    if (((IntWritable) getFromMap(agg, TYPE_MESSAGE)).get() == 0) {
                        ((CustomMapWritable) this.getValue().get(new IntWritable(PROPAGATION_DONE))).clear();
                    }
                }

                for (CustomMapWritable cmw : messages) {
                    if (getSuperstepCount() == 0) {
// each node sends it's id and that of the neigh
                        int message_type = getMessageType(cmw);
                        if (message_type == ACTIVATION_MESSAGE) {
                            //  System.out.println(this.getSuperstepCount());
                            this.getValue().put(new IntWritable(PROBABILITY_INDEX), new DoubleWritable(1));
                            nodeBoot();
                        }
                    }
                    int message_type = getMessageType(cmw);
//  indicate that a node is activating
                    if (message_type == ACTIVATE) {

                        this.getValue().put(new IntWritable(INFLUENCE_SOURCE), getFromMap(cmw, INFLUENCE_SOURCE));
                        this.getValue().put(new IntWritable(DISTANCE_INDEX), getFromMap(cmw, DISTANCE_INDEX));
                        CustomMapWritable donPro = ((CustomMapWritable) getFromMap(this.getValue(), PROPAGATION_DONE));
// of the influence for that node source has already been processed, don't have to do it again, it is involved once
                        if (!donPro.containsKey(((IntWritable) getFromMap(this.getValue(), INFLUENCE_SOURCE)))) {
                            this.getValue().put(new IntWritable(PROBABILITY_INDEX), ((DoubleWritable)getFromMap(cmw, PROBABILITY_INDEX)));
                            tryToInfluenceNodes();
                            donPro.put(getFromMap(this.getValue(), INFLUENCE_SOURCE), new BooleanWritable(true));
                            this.getValue().put(new IntWritable(PROPAGATION_DONE), donPro);
                        }

//propagation will restart, sending data to master
                    }
                    if (message_type == UPDATE_DATA) {
                        sendNeighToMaster();

                    }

                }
// -2 aggregated is the stopping signal from the master
                CustomMapWritable aggregate_value = this.getAggregatedValue(0);
                if (this.getSuperstepCount() > 1) {
                    if (((IntWritable) aggregate_value.get(new IntWritable(TYPE_MESSAGE))).get() == -2) {
                        voteToHalt();
                    }
                }
            } else {

                //bootstrap, wait for all seed nodes
                if (!((BooleanWritable) getFromMap(this.getValue(), ALL_SEED_SELECTED)).get()) {
                    masterBoot(messages);
                    Writable[] seed_list = ((IntArrayWritable) getFromMap(this.getValue(), MASTER_SEED_LIST_INDEX)).get();
                    boolean completed = true;
                    for (Writable i : seed_list) {
                        if (((IntWritable) i).get() == -1) {
                            completed = false;
                        }
                    }
                    if (completed) {
                        sendStartToSeed();
                        this.getValue().put(new IntWritable(ALL_SEED_SELECTED), new BooleanWritable(true));
                    }

                    if (this.getSuperstepCount() > 3 && !completed) {
                        throw new ExceptionInInitializerError();

                    }
                } else {
                  
                    boolean restart = false;
                    for (CustomMapWritable mex : messages) {
                        int message_type = ((IntWritable) getFromMap(mex, TYPE_MESSAGE)).get();
                      //master receives this kind of message also from a non activated node to determine that propagation is in progress
                        if (message_type == -2) {
                            this.getValue().put(new IntWritable(IS_RUNNING_PROPAGATION), new BooleanWritable(true));

                        }
                        //in this beta version of the graph job, there's no aggregator to evaluate fitness, each node send influence data to the master
                        //that stores this value to evaluate at the end the fitness
                        if (message_type == UPDATE_INFLUENCE) {
                            this.getValue().put(new IntWritable(IS_RUNNING_PROPAGATION), new BooleanWritable(true));
                            parseUpdatePropagationMessage(mex);

                        }
                        // neigh. update
                        if (message_type == UPDATE_DATA) {
                            parseData(mex);
                            restart = true;
                        }

                    }
                    
                    if (restart) {
                        this.getValue().put(new IntWritable(NEIGH_NUMBER), new IntWritable(0));
                        this.getValue().put(new IntWritable(IS_RUNNING_PROPAGATION), new BooleanWritable(true));

                    }
                    //establishes if the propagation is ended
                    if (((BooleanWritable) getFromMap(this.getValue(), IS_RUNNING_PROPAGATION)).get()) {
                         CustomMapWritable agg = getAggregatedValue(0);
                        //selecte next SB
                        if (((IntWritable) getFromMap(agg, TYPE_MESSAGE)).get() == 0) {
                            this.getValue().put(new IntWritable(IS_RUNNING_PROPAGATION), new BooleanWritable(false));
                            selectFirstNeighNode();

                        }
                    }
                }

            }

        }

        private void parseData(CustomMapWritable cmw) {
            int vertex_sender = ((IntWritable) getFromMap(cmw, SENDER_NODE)).get();
            Writable[] seed_list = ((IntArrayWritable) getFromMap(this.getValue(), MASTER_SEED_LIST_INDEX)).get();
            IntArrayWritable aw = ((IntArrayWritable) getFromMap(cmw, NEIGH_LIST));
            Writable[] nlist = (Writable[]) aw.get();
            CustomMapWritable rankReceived = ((CustomMapWritable) getFromMap(cmw, RANKING_INDEX));
            CustomMapWritable biMap = ((CustomMapWritable) getFromMap(this.getValue(), NEIGH_SET_INDEX));
            if(topForNeigh){
                LinkedList<Integer>ll=new LinkedList<Integer>();
                for(Writable i: nlist){
                    ll.add(((IntWritable)i).get());
                }
                Collections.sort(ll, new Comparator<Integer>() {
                   CustomMapWritable tempRank=rankReceived;
                    @Override
                    public int compare(Integer t, Integer t1) {
                        double val1=((DoubleWritable)tempRank.get(new IntWritable(t))).get();
                        double val2=((DoubleWritable)tempRank.get(new IntWritable(t1))).get();
                        if(val1>=val2)
                            return -1;
                       
                        return 1;
                    }
                });
                for(int i=0; i<nlist.length; i++){
                    nlist[i]=new IntWritable(ll.get(i));
                }
                
                
            }
            for (Writable i : nlist) {
                    biMap.put(new IntWritable(Integer.parseInt(i.toString())), rankReceived.get(new IntWritable(Integer.parseInt(i.toString()))));
                    cont=cont+1;
            }
            
            
            for (Writable seed : seed_list) {
                try {
                    biMap.get((IntWritable) seed);
                    biMap.remove((IntWritable) seed);
                } catch (Exception e) {
                }
            }

            this.getValue().put(new IntWritable(NEIGH_SET_INDEX), biMap);
                CustomMapWritable mineMapT = ((CustomMapWritable) getFromMap(this.getValue(), NEIGH_SET_INDEX));
            for (Writable key : mineMapT.keySet()) {
            }
        }

        private void sendNeighToMaster() {
            CustomMapWritable cmp = new CustomMapWritable();

            IntArrayWritable aw = new IntArrayWritable();
            IntWritable[] nlist = new IntWritable[this.getEdges().size()];
            int i = 0;
            for (Edge e : this.getEdges()) {
                nlist[i] = new IntWritable(Integer.parseInt(e.getDestinationVertexID().toString()));
                i += 1;
            }
            aw.set(nlist);
            CustomMapWritable rankMap = new CustomMapWritable();
            for (Edge e : this.getEdges()) {
                TextPair val = (TextPair) e.getValue();
                rankMap.put(new IntWritable(Integer.parseInt(e.getDestinationVertexID().toString())), new DoubleWritable(Double.parseDouble(val.getSecond().toString())));
            }
            cmp.put(new IntWritable(TYPE_MESSAGE), new IntWritable(UPDATE_DATA));
            cmp.put(new IntWritable(SENDER_NODE), new IntWritable(Integer.parseInt(this.getVertexID().toString())));
            cmp.put(new IntWritable(NEIGH_LIST), aw);
            cmp.put(new IntWritable(RANKING_INDEX), rankMap);

            try {
                sendMessage(master, cmp);
            } catch (IOException ex) {
                Logger.getLogger(WABCFW.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        private void selectFirstNeighNode() throws IOException {

            boolean checkSwitch = ((BooleanWritable) getFromMap(this.getValue(), SWITCH_EVAL)).get();
            if (checkSwitch) {
                updateSeedNode();
            }
              int num_proc = ((IntWritable) getFromMap(this.getValue(), NEIGH_NUMBER)).get();
            if (num_proc == MAX_NEIGH) {
                ((CustomMapWritable) getFromMap(this.getValue(), NEIGH_SET_INDEX)).clear();
            } else {
                this.getValue().put(new IntWritable(NEIGH_NUMBER), new IntWritable(num_proc + 1));
            }
            CustomMapWritable neighMap = (CustomMapWritable) getFromMap(this.getValue(), NEIGH_SET_INDEX);

            double maximum = -1;
            int vertex = -1;
            for (Writable key : neighMap.keySet()) {
                double value = ((DoubleWritable) neighMap.get(new IntWritable(Integer.parseInt(key.toString())))).get();
                if (value > maximum) {
                    maximum = value;
                    vertex = Integer.parseInt(key.toString());
                }
            }

            if (vertex != -1) {
                neighMap.remove(new IntWritable(vertex));
                this.getValue().put(new IntWritable(SWITCH_NAME_NODE), new IntWritable(vertex));
                this.getValue().put(new IntWritable(SWITCH_EVAL), new BooleanWritable(true));
                CustomMapWritable cmp = new CustomMapWritable();
                this.getValue().put(new IntWritable(SWITCH_NODE_INFLUENCE), cmp);
                CustomMapWritable messageForNeigh = new CustomMapWritable();
                messageForNeigh.put(new IntWritable(TYPE_MESSAGE), new IntWritable(ACTIVATE));
                messageForNeigh.put(new IntWritable(INFLUENCE_SOURCE), new IntWritable(vertex));
                messageForNeigh.put(new IntWritable(DISTANCE_INDEX), new IntWritable(DISTANCE));
                messageForNeigh.put(new IntWritable(PROBABILITY_INDEX), new DoubleWritable(1.0));
                try {

                    this.sendMessage(new Text(String.valueOf(vertex)), messageForNeigh);
                } catch (IOException ex) {
                    Logger.getLogger(WABCFW.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else {
                stopProcess();
            }
        }

        private void updateSeedNode() {
            CustomMapWritable influenced_from_neigh = ((CustomMapWritable) getFromMap(this.getValue(), SWITCH_NODE_INFLUENCE));
            HashMap<Integer, Double> uniqueNodes = new HashMap<Integer, Double>();
            CustomMapWritable seed_influence = ((CustomMapWritable) getFromMap(this.getValue(), INFLUENCED_INDEX));
            for (Writable key : seed_influence.keySet()) {
                ((CustomMapWritable) seed_influence.get((IntWritable) key)).put((IntWritable) key, new DoubleWritable(1));

            }

            influenced_from_neigh.put(getFromMap(this.getValue(), SWITCH_NAME_NODE), new DoubleWritable(1));
            for (Writable key : seed_influence.keySet()) {
                double counter = 0;
                CustomMapWritable influencedBySeed = ((CustomMapWritable) seed_influence.get((IntWritable) key));
                for (Writable kk : influencedBySeed.keySet()) {
                    boolean contained = false;
                    for (Writable second_key : seed_influence.keySet()) {

                        if (second_key != key) {
                            CustomMapWritable second_key_map = (CustomMapWritable) seed_influence.get((IntWritable) second_key);
                            if ((second_key_map.containsKey((IntWritable) kk)) || ((IntWritable) second_key).get() == ((IntWritable) kk).get()) {
                                contained = true;
                                break;
                            }
                        }
                        if (contained) {
                            break;
                        }
                    }
                    for (Writable item : influenced_from_neigh.keySet()) {
                        if (((IntWritable) item).get() == ((IntWritable) kk).get() || ((IntWritable) kk).get() == ((IntWritable) getFromMap(this.getValue(), SWITCH_NAME_NODE)).get()) {
                            contained = true;
                            break;
                        }
                    }
                    if (!contained) {
                        if(weightedAlgorithm){
                            counter+=1*((DoubleWritable)influencedBySeed.get(kk)).get();
                            
                        }else{
                        counter += 1;
                        }
                        }
            
                }
                uniqueNodes.put(Integer.parseInt(key.toString()), counter);
            }
              double counter = 0;
            for (Writable key : influenced_from_neigh.keySet()) {
                boolean contained = false;
                for (Writable second_key : seed_influence.keySet()) {
                    CustomMapWritable second_key_map = ((CustomMapWritable) seed_influence.get((IntWritable) second_key));
                    for (Writable item : second_key_map.keySet()) {
                        if (((IntWritable) item).get() == ((IntWritable) key).get() || ((IntWritable) key).get() == ((IntWritable) second_key).get()) {
                            contained = true;
                            break;
                        }
                    }
                    if (contained) {
                        break;
                    }
                }

                if (!contained) {
                    if(weightedAlgorithm){
                        counter+=1*((DoubleWritable)influenced_from_neigh.get(key)).get();
                    }else
                    counter += 1;
                }
            }
            int name_influencers = ((IntWritable) getFromMap(this.getValue(), SWITCH_NAME_NODE)).get();

    
            double minimum = Double.MAX_VALUE;
            int key = -2;
            for (Integer it : uniqueNodes.keySet()) {
                if (uniqueNodes.get(it) < minimum) {
                    minimum = uniqueNodes.get(it);
                    key = it;
                }
            }
            boolean switched = false;

            if (counter > minimum) {
                switched = true;

                Writable[] seed_list = ((IntArrayWritable) getFromMap(this.getValue(), MASTER_SEED_LIST_INDEX)).get();

                for (int i = 0; i < seed_list.length; i++) {
                    IntWritable x = (IntWritable) seed_list[i];
                     if (x.get() == key) {
                        seed_list[i] = new IntWritable(name_influencers);
                        break;
                    }
                }

            }

            if (switched) {
               
                this.getValue().put(new IntWritable(CONV_SWITCH), new BooleanWritable(true));
                CustomMapWritable temp = ((CustomMapWritable) getFromMap(this.getValue(), INFLUENCED_INDEX));
                temp.remove(new IntWritable(key));
                CustomMapWritable inf_N = new CustomMapWritable();
                for (Writable as : influenced_from_neigh.keySet()) {
                    inf_N.put(as, influenced_from_neigh.get(as));
                }
                temp.put(new IntWritable(name_influencers), inf_N);
               
            }

            ((CustomMapWritable) getFromMap(this.getValue(), SWITCH_NODE_INFLUENCE)).clear();

        }

        private void sendEnd() throws IOException {
            CustomMapWritable end_map = new CustomMapWritable();
            end_map.put(new IntWritable(TYPE_MESSAGE), new IntWritable(-2));
            this.aggregate(0, end_map);
        }

        private void stopProcess() throws IOException {

            
            
            double actualVal=0;
            CustomMapWritable inf= (CustomMapWritable)getFromMap(this.getValue(), INFLUENCED_INDEX);
            for(Writable i: inf.keySet()){
                CustomMapWritable infl =(CustomMapWritable)inf.get(i);
                for(Writable x: infl.keySet()){
                    actualVal+=1*((DoubleWritable)infl.get(x)).get();
                }
            }
            
            double old_val= ((DoubleWritable)getFromMap(this.getValue(), OLD_VAL)).get();
            double convergence_val= old_val*STOPPING_PERC_VALUE;
            this.getValue().put(new IntWritable(OLD_VAL), new DoubleWritable(actualVal));
            if (!((BooleanWritable) getFromMap(this.getValue(), CONV_SWITCH)).get()|| (Math.abs(actualVal-old_val))<=convergence_val) {
                sendEnd();
                Writable[] final_seed = ((IntArrayWritable) getFromMap(this.getValue(), MASTER_SEED_LIST_INDEX)).get();
                
                CustomMapWritable coperturaFinale= new CustomMapWritable();
                CustomMapWritable seedCop= ((CustomMapWritable)getFromMap(this.getValue(), INFLUENCED_INDEX));

                for(Writable key : seedCop.keySet()){
                                    
                    
                    for(Writable key2: ((CustomMapWritable)seedCop.get(key)).keySet()){
                        coperturaFinale.put(key2, new BooleanWritable(true));
                    }
                    
                    }
                voteToHalt();
            } else {
                this.getValue().put(new IntWritable(SWITCH_EVAL), new BooleanWritable(false));
                Writable[] seed_list = ((IntArrayWritable) getFromMap(this.getValue(), MASTER_SEED_LIST_INDEX)).get();
                CustomMapWritable message = new CustomMapWritable();
                message.put(new IntWritable(TYPE_MESSAGE), new IntWritable(UPDATE_DATA));
                for (Writable i : seed_list) {
                    if (((IntWritable) i).get() != -1) {
                        this.sendMessage(new Text(i.toString()), message);
                    }
                }
                this.getValue().put(new IntWritable(CONV_SWITCH), new BooleanWritable(false));
                ((CustomMapWritable) this.getValue().get(new IntWritable(NEIGH_SET_INDEX))).clear();

            }
        }

        private void parseUpdatePropagationMessage(CustomMapWritable mex) {
            if (!((BooleanWritable) getFromMap(this.getValue(), SWITCH_EVAL)).get()) {
                int influence_source = ((IntWritable) getFromMap(mex, INFLUENCE_SOURCE)).get();
                int vertex_dest = ((IntWritable) getFromMap(mex, INFLUENCED_NODE)).get();
                double probability= ((DoubleWritable)getFromMap(mex, PROBABILITY_INDEX)).get();
                CustomMapWritable infMap = ((CustomMapWritable) getFromMap(this.getValue(), INFLUENCED_INDEX));
                CustomMapWritable mapVertexSource = ((CustomMapWritable) infMap.get(new IntWritable(influence_source)));
                mapVertexSource.put(new IntWritable(vertex_dest), new DoubleWritable(probability));
                CustomMapWritable temp = ((CustomMapWritable) getFromMap(this.getValue(), INFLUENCED_INDEX));
                for (Writable key : temp.keySet()) {
                    for (Writable key2 : ((CustomMapWritable) temp.get(key)).keySet()) {
                    }
                }

            } else {
                int influence_source = ((IntWritable) getFromMap(mex, INFLUENCE_SOURCE)).get();
                int vertex_dest = ((IntWritable) getFromMap(mex, INFLUENCED_NODE)).get();
                double probability=((DoubleWritable)getFromMap(mex, PROBABILITY_INDEX)).get();
                CustomMapWritable infMap = ((CustomMapWritable) getFromMap(this.getValue(), SWITCH_NODE_INFLUENCE));
                infMap.put(new IntWritable(vertex_dest), new DoubleWritable(probability));

                CustomMapWritable temp = ((CustomMapWritable) getFromMap(this.getValue(), SWITCH_NODE_INFLUENCE));
            }
        }

        private void tryToInfluenceNodes() throws IOException {
            boolean sended = false;
            int distance = ((IntWritable) getFromMap(this.getValue(), DISTANCE_INDEX)).get();
            if (distance == 0) {
                return;
            }
            for (Edge e : this.getEdges()) {
                
                TextPair val = (TextPair) e.getValue();
                double weight = Double.parseDouble(val.getFirst().toString());
                double actualProb=((DoubleWritable)getFromMap(this.getValue(), PROBABILITY_INDEX)).get();
                int goodSampling=0;
                int current_simulation=0;
                while(current_simulation<limit){
                double sample = Math.random();
                if(sample<=weight){
                    goodSampling++;}
                    current_simulation++;
                }
                
                //-1 IS for fake return edges
                if(weight!=-1){
                if (goodSampling>threshold) {
                    CustomMapWritable message = new CustomMapWritable();
                    message.put(new IntWritable(TYPE_MESSAGE), new IntWritable(ACTIVATE));
                    message.put(new IntWritable(INFLUENCE_SOURCE), (IntWritable) getFromMap(this.getValue(), INFLUENCE_SOURCE));
                    message.put(new IntWritable(DISTANCE_INDEX), new IntWritable(distance - 1));
                    message.put(new IntWritable(PROBABILITY_INDEX), new DoubleWritable(actualProb*weight));
                    
                    aggregate(0, message);
                    this.sendMessage(e, message);
                    sendInfluenceUpdateToMaster(e,actualProb*weight);
                    sended = true;
                }
                }
            }
            if (!sended) {
                CustomMapWritable cmReserve = new CustomMapWritable();
                cmReserve.put(new IntWritable(TYPE_MESSAGE), new IntWritable(-2));
                sendMessage(master, cmReserve);
            }
            
                
        }

        private void sendInfluenceUpdateToMaster(Edge e, double weight) throws IOException {
            sendMessage(master, createMessageForInfluenceUpdateToMaster(e, weight));

        }

        private CustomMapWritable createMessageForInfluenceUpdateToMaster(Edge e, double weight) {
             CustomMapWritable message = new CustomMapWritable();
            message.put(new IntWritable(TYPE_MESSAGE), new IntWritable(UPDATE_INFLUENCE));
            message.put(new IntWritable(INFLUENCE_SOURCE), (IntWritable) getFromMap(this.getValue(), INFLUENCE_SOURCE));
            message.put(new IntWritable(INFLUENCED_NODE), new IntWritable(Integer.parseInt(e.getDestinationVertexID().toString())));
            message.put(new IntWritable(PROBABILITY_INDEX), new DoubleWritable(weight));
            return message;
        }

        private void sendStartToSeed() throws IOException {

            Writable[] seed_list = ((IntArrayWritable) getFromMap(this.getValue(), MASTER_SEED_LIST_INDEX)).get();
            CustomMapWritable nei = ((CustomMapWritable) getFromMap(this.getValue(), NEIGH_SET_INDEX));
            for (Writable w : seed_list) {
                try {
                    nei.get((IntWritable) w);
                    nei.remove((IntWritable) w);
                } catch (Exception e) {

                }
            }
            CustomMapWritable message_map = new CustomMapWritable();
            message_map.put(new IntWritable(TYPE_MESSAGE), new IntWritable(ACTIVATE));
            message_map.put(new IntWritable(DISTANCE_INDEX), new IntWritable(DISTANCE));
            message_map.put(new IntWritable(PROBABILITY_INDEX), new DoubleWritable(1));
            for (Writable seed : seed_list) {
                message_map.put(new IntWritable(INFLUENCE_SOURCE), new IntWritable(Integer.parseInt(seed.toString())));
              
                this.sendMessage(new Text(seed.toString()), message_map);
                System.out.println(seed.toString() + " INVIATO ");
            }
        }

        //BOOT METHODS
        private void masterBoot(Iterable<CustomMapWritable> messages) {
            for (CustomMapWritable cmw : messages) {
                int messageType = getMessageType(cmw);
                if (messageType == ACTIVATION_INFO) {
                    int vertex_sender = ((IntWritable) getFromMap(cmw, SENDER_NODE)).get();
                    IntArrayWritable aw = ((IntArrayWritable) getFromMap(cmw, NEIGH_LIST));
                    Writable[] nlist = (Writable[]) aw.get();
                    Writable[] seedList = ((IntArrayWritable) getFromMap(this.getValue(), MASTER_SEED_LIST_INDEX)).get();
                    seedList[((IntWritable) getFromMap(this.getValue(), SEED_POINTER)).get()] = new IntWritable(vertex_sender);
                    CustomMapWritable rankReceived = ((CustomMapWritable) getFromMap(cmw, RANKING_INDEX));
                    CustomMapWritable biMap = ((CustomMapWritable) getFromMap(this.getValue(), NEIGH_SET_INDEX));
                   
                    if(topForNeigh){
                        LinkedList<Integer>val=new LinkedList<Integer>();
                        for(Writable i: nlist){
                            val.add(((IntWritable)i).get());
                        }
                        Collections.sort(val, new Comparator<Integer>() {
                           CustomMapWritable tempRank=rankReceived;
                            @Override
                            public int compare(Integer t, Integer t1) {
                                double val1=((DoubleWritable)tempRank.get(new IntWritable(t))).get();
                                double val2=((DoubleWritable)tempRank.get(new IntWritable(t1))).get();
                                if(val1>=val2){

                                    return -1;
                                }
                                return 1;
                            
                            }
                        });
                        for(int i=0; i<nlist.length;i++){
                           nlist[i]=new IntWritable(val.get(i));
                           
                       }
                        
                    }
                    int cont=0;
                    for (Writable i : nlist) {
                        if(topForNeigh&&cont>=number_of_top)
                            break;
                       
                        biMap.put(new IntWritable(Integer.parseInt(i.toString())), rankReceived.get(new IntWritable(Integer.parseInt(i.toString()))));
                        cont++;
                    }
                    this.getValue().put(new IntWritable(NEIGH_SET_INDEX), biMap);

                    int pointer = ((IntWritable) getFromMap(this.getValue(), SEED_POINTER)).get();
                    this.getValue().put(new IntWritable(SEED_POINTER), new IntWritable(pointer + 1));
                    ((CustomMapWritable) getFromMap(this.getValue(), INFLUENCED_INDEX)).put(new IntWritable(vertex_sender), new CustomMapWritable());
                    CustomMapWritable mineMapT = ((CustomMapWritable) getFromMap(this.getValue(), NEIGH_SET_INDEX));
                }
            }

        }

        private void nodeBoot() {
            CustomMapWritable cmp = new CustomMapWritable();

            IntArrayWritable aw = new IntArrayWritable();
            IntWritable[] nlist = new IntWritable[this.getEdges().size()];
            int i = 0;
            for (Edge e : this.getEdges()) {
                nlist[i] = new IntWritable(Integer.parseInt(e.getDestinationVertexID().toString()));
                i += 1;
            }
            aw.set(nlist);
            CustomMapWritable rankMap = new CustomMapWritable();
            for (Edge e : this.getEdges()) {
                TextPair val = (TextPair) e.getValue();
                rankMap.put(new IntWritable(Integer.parseInt(e.getDestinationVertexID().toString())), new DoubleWritable(Double.parseDouble(val.getSecond().toString())));
            }
            
            cmp.put(new IntWritable(TYPE_MESSAGE), new IntWritable(ACTIVATION_INFO));
            cmp.put(new IntWritable(SENDER_NODE), new IntWritable(Integer.parseInt(this.getVertexID().toString())));
            cmp.put(new IntWritable(NEIGH_LIST), aw);
            cmp.put(new IntWritable(RANKING_INDEX), rankMap);

            try {
                sendMessage(master, cmp);
            } catch (IOException ex) {
                Logger.getLogger(WABCFW.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        // UTIL METHODS
        private int getMessageType(CustomMapWritable cmp) {
            return ((IntWritable) cmp.get(new IntWritable(TYPE_MESSAGE))).get();
        }

        private Writable getFromMap(CustomMapWritable cmp, int index) {
            return cmp.get(new IntWritable(index));
        }

        private boolean isMaster() {
            return this.getVertexID().toString().equals("-1");
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
            CustomMapWritable cmp = new CustomMapWritable();
            vertex.setVertexID(new Text(jsonArray.get(0).toString()));

            String val = jsonArray.get(1).toString();
            Double rank = Double.parseDouble(jsonArray.get(2).toString());
            cmp.put(new IntWritable(0), new IntWritable(Integer.parseInt(val)));
            cmp.put(new IntWritable(1), new DoubleWritable(rank));

            vertex.setValue(cmp);

            Iterator<JSONArray> iter = ((JSONArray) jsonArray.get(3)).iterator();
            while (iter.hasNext()) {
                JSONArray edge = (JSONArray) iter.next();
                TextPair p = new TextPair();
                p.setFirst(new Text(edge.get(1).toString()));
                p.setSecond(new Text(edge.get(2).toString()));
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

        GraphJob pageJob = new GraphJob(conf, WABCFW.class);
        pageJob.setJobName("WABC");

        pageJob.setVertexClass(WABC.class);
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
