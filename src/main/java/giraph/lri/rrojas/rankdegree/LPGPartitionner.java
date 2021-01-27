/**
 * Copyright 2018 SpinnerPlusPlus.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package giraph.lri.rrojas.rankdegree;

import it.unimi.dsi.fastutil.shorts.ShortArrayList;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.counters.GiraphTimers;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Task;

import com.google.common.collect.Lists;
/*
import giraph.lri.rrojas.rankdegree.BGRAP_vb.ComputeFirstMigration;
import giraph.lri.rrojas.rankdegree.BGRAP_vb.ComputeFirstPartition;
import giraph.lri.rrojas.rankdegree.BGRAP_vb.ComputeMigration;
import giraph.lri.rrojas.rankdegree.BGRAP_vb.ComputeNewPartition;
//*/
import giraph.lri.rrojas.rankdegree.BGRAP_eb.ComputeFirstMigration;
import giraph.lri.rrojas.rankdegree.BGRAP_eb.ComputeFirstPartition;
import giraph.lri.rrojas.rankdegree.BGRAP_eb.ComputeMigration;
import giraph.lri.rrojas.rankdegree.BGRAP_eb.ComputeNewPartition;
import giraph.lri.rrojas.rankdegree.HashMapAggregator;
import giraph.lri.rrojas.rankdegree.LPGPartitionner.ComputeGraphPartitionStatistics;
import giraph.lri.rrojas.rankdegree.LPGPartitionner.ConverterPropagate;
import giraph.lri.rrojas.rankdegree.LPGPartitionner.ConverterUpdateEdges;
import giraph.lri.rrojas.rankdegree.LPGPartitionner.Repartitioner;
import giraph.lri.rrojas.rankdegree.LPGPartitionner.SuperPartitionerMasterCompute;
import giraph.lri.rrojas.rankdegree.Samplers.*;
import giraph.lri.rrojas.rankdegree.SamplingMessage;
import giraph.ml.grafos.okapi.spinner.EdgeValue;
import giraph.ml.grafos.okapi.spinner.PartitionMessage;
import giraph.ml.grafos.okapi.spinner.VertexValue;

/**
 *
 * 
 */
@SuppressWarnings("unused")
public class LPGPartitionner {
	//GENERAL VARIABLES
	public static final String SAVE_PATH = "/home/ubuntu/BDRP_Graph_Partitioning/Results/";
	public static final String SAVE_STATS = "partition.SaveStatsIntoFile";
	
	
	//ALGORITHM PARAMETERS
		//execution and graph environment
	public static final String MAX_ITERATIONS_LP = "spinner.MaxIterationsLP";
	protected static final int DEFAULT_MAX_ITERATIONS = 290;// 10;//290;
	public static final String GRAPH_DIRECTED = "graph.directed";
	public static final boolean DEFAULT_GRAPH_DIRECTED = false;
	public static final String EDGE_WEIGHT = "spinner.weight";
	public static final byte DEFAULT_EDGE_WEIGHT = 1;														//RR: Shouldn't edge weights be respected? It helps convergence speed

		//initialization degreeThreshold																	
	protected static int outDegreeThreshold;
	public static final String COMPUTE_OUTDEGREE_THRESHOLD = "partition.OUTDEGREE_THRESHOLD";				// makes the algorithm set the threshold for initialization
	protected static final boolean DEFAULT_COMPUTE_OUTDEGREE_THRESHOLD = true;
	public static final String OUTDEGREE_THRESHOLD = "partition.OUTDEGREE_THRESHOLD";						//RR: What is the use of this?! it will initialize all nodes
	protected static final int DEFAULT_OUTDEGREE_Threshold = 1;
	public static final String MIN_OUTDEGREE_THRESHOLD = "partition.MIN_OUTDEGREE_THRESHOLD";				//RR: completely useless
	protected static final int DEFAULT_MIN_OUTDEGREE_Threshold = 25;										//RR: completely useless
	
	
		//user objectives
	public static final String NUM_PARTITIONS = "spinner.numberOfPartitions";
	protected static final int DEFAULT_NUM_PARTITIONS = 8;//32;
	protected static final String REPARTITION = "spinner.repartition";
	protected static final short DEFAULT_REPARTITION = 0;
	protected static final String ADDITIONAL_CAPACITY = "spinner.additionalCapacity";
	protected static final float DEFAULT_ADDITIONAL_CAPACITY = 0.05f;
	
		//convergence
	protected static final String LAMBDA = "spinner.lambda";												// constraints the penalty value => Why is it added as well to H?
	protected static final float DEFAULT_LAMBDA = 1.0f;
	public static final String KAPPA = "partition.edgeBalanceWeight.kappa";									// for scoring, in vb gives more priority to vb or ec
	protected static final float DEFAULT_KAPPA = 0.5f;
	protected static final String WINDOW_SIZE = "spinner.windowSize";
	protected static final int DEFAULT_WINDOW_SIZE = 5;
	protected static final String CONVERGENCE_THRESHOLD = "spinner.threshold";
	protected static final float DEFAULT_CONVERGENCE_THRESHOLD = 0.001f;
	
	//AGGREGATORS
		//score
	protected static final String AGGREGATOR_STATE = "AGG_STATE";											// To calculate overall solution score
	
		//migration
	protected static final String AGGREGATOR_MIGRATIONS = "AGG_MIGRATIONS";									// Total of migrating v (for eb and vb)
	protected static final String AGG_MIGRATION_DEMAND_PREFIX = "AGG_EDGE_MIGRATION_DEMAND_";				// only for eb
	protected static final String AGG_VERTEX_MIGRATION_DEMAND_PREFIX = "AGG_VERTEX_MIGRATION_DEMAND_";		// only for vb

		//edges
	protected static final String TOTAL_DIRECTED_OUT_EDGES = "#Total Directed Out Edges";					// initial graph out-edges
	
	protected static final String AGG_EGDES_LOAD_PREFIX = "AGG_LOAD_";										// Edges in a partition for balancing
	protected static final String AGG_REAL_LOAD_PREFIX = "AGG_REAL_LOAD_";
	protected static final String AGG_VIRTUAL_LOAD_PREFIX = "AGG_VIRTUAL_LOAD_";
	
	protected static final String AGGREGATOR_LOCALS = "AGG_LOCALS";											// Local edges (intra-partition)
	protected static final String AGG_REAL_LOCAL_EDGES = "AGG_REAL_LOCALS";
	protected static final String AGG_VIRTUAL_LOCALS = "AGG_VIRTUAL_LOCALS";

	protected static final String AGG_EDGE_CUTS = "# Edge cuts";											// External edges (a.k.a. edge-cut)
	protected static final String AGG_REAL_EDGE_CUTS = "#REAL Edge cuts";
	protected static final String AGG_VIRTUAL_EDGE_CUTS = "#VIRTUAL Edge cuts";
	
	//AUTRES
		//OUTDEGREE_FREQUENCY_COUNTER => not even defined, it is for the histogram
	
	

	protected static final String COUNTER_GROUP = "Partitioning Counters";
	protected static final String MIGRATIONS_COUNTER = "Migrations";
	protected static final String ITERATIONS_COUNTER = "Iterations";
	protected static final String PCT_LOCAL_EDGES_COUNTER = "Local edges (%)";
	protected static final String MAXMIN_UNBALANCE_COUNTER = "Maxmin unbalance (x1000)";
	protected static final String MAX_NORMALIZED_UNBALANCE_COUNTER = "Max normalized unbalance (x1000)";
	protected static final String SCORE_COUNTER = "Score (x1000)";

	protected static final String PARTITION_COUNTER_GROUP = "Partitioning Counters";
	// Adnan
	protected static int totalVertexNumber; //RR: changed to int
	// Adnan : ration Communication Computation
	

	// Adnan: some statistics about the initialization
	protected static final String AGG_INITIALIZED_VERTICES = "Initialized vertex %";
	protected static final String AGG_UPDATED_VERTICES = "Updated vertex %";
	// Adnan : the outgoing edges of initialized vertices
	protected static final String AGG_FIRST_LOADED_EDGES = "First Loaded Edges %";

	// Adnan: some statistics about the partition
	// global stat
	
	protected static final String AGG_UPPER_TOTAL_COMM_VOLUME = "# CV Upper Bound";
	
	// local stat (per partition)
	protected static final String FINAL_AGG_VERTEX_COUNT_PREFIX = "FINAL_AGG_VERTEX_CAPACITY_";
	protected static final String AGG_VERTEX_COUNT_PREFIX = "AGG_VERTEX_CAPACITY_";
	protected static final String AGG_AVG_DEGREE_PREFIX = "AGG_AVG_DEGREE_CAPACITY_";

	public static final String DEBUG = "graph.debug";

	// Adnan: some statistics about the partition	

	protected static final String AGG_REAL_TOTAL_COMM_VOLUME = "REAL Communication Volume";
	protected static final String AGG_VIRTUAL_TOTAL_COMM_VOLUME = "VIRTUAL Communication Volume";

	protected static final String AGG_REAL_UPPER_TOTAL_COMM_VOLUME = "#REAL CV Upper Bound";
	private static final String AGG_VIRTUAL_UPPER_TOTAL_COMM_VOLUME = "#VIRTUAL CV Upper Bound";

	public static final String Vertex_Balance_JSD_COUNTER = "Vertex Balance JSD";
	public static final String Edge_Balance_JSD_COUNTER = "Edge Balance JSD";
	public static final String OUTDEGREE_FREQUENCY_COUNTER = "OUTDEGREE_FREQUENCY_COUNTER_";
	
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//RR VARIABLES //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////	
	
	//Variables for saving results in files
	protected static final String DELIMITER = ",";
	public static final String DATE_HANDLE = "bgrap.DATE";
	protected static String formattedDate;
	public static final String GRAPH_NAME_HANDLE = "bgrap.GRAPH";
	protected static String GRAPH_NAME="Unknown";
	public static final String SAVE_DD_HANDLE = "rankdegree.SAVE_DD";
	protected static String SAVE_DD="false";
	
	//RANK DEGREE SAMPLING
	public static boolean NEEDS_SAMPLE;	
	public static final String SAMPLING_TYPE_HANDLE = "bgrap.SAMPLING_TYPE";
	protected static final String DEFAULT_SAMPLING_TYPE = "InitializeSampleRD";
	protected static String SAMPLING_TYPE;
	//number of sampled vertices
	public static final String BETA_HANDLE = "rankdegree.BETA";
	protected static final float BETA_DEFAULT = 0.15f;
	protected static float BETA_P;
	protected static int BETA;
	//number of seeds
	public static final String SIGMA_HANDLE = "rankdegree.SIGMA";
	protected static final float SIGMA_DEFAULT = 0.05f;
	protected static float SIGMA_P;
	protected static int SIGMA;
	//number of neighbors taken
	public static final String TAU_HANDLE = "rankdegree.TAU";
	protected static final int TAU_DEFAULT = 3;
	protected static int TAU;

	//protected static boolean[] initializedPartition;
	protected static Random r = new Random();
	
	//VARIABLE NAMERS
	//for degree distribution
	public static final String VIRTUAL_DEGREE_FREQUENCY_COUNTER = "VIRTUAL_DEGREE_FREQUENCY_COUNTER_";
	
	//AGGREGATORS
	protected static final String AGG_VERTICES = "VERTICES";
	protected static final String AGG_MAX_DEGREE = "MAX_DEGREE";
	protected static final String AGG_DEGREE_DIST = "DEGREE_DIST";
	protected static final String AGG_SAMPLE = "SAMPLE";
	protected static final String AGG_SAMPLE_SS = "SAMPLED_IN_SUPERSTEP";
	protected static final String AGG_SAMPLE_SSR = "SAMPLED_IN_SUPERSTEP_FOR_REAL";
	
	//RESULTS
	//super steps 
	protected static short sampling_ss_start = 2;
	protected static short sampling_ss_extra = 0; //super steps to guarantee partition initializing
	protected static short sampling_ss_end;
	protected static short lp_ss_end;
		
	//messages
	protected static long sampling_messages_start;
	protected static long sampling_messages_end;
	protected static long lp_messages_end;
	
	//ALGORITHM SPECIFIC
	//Hashmaps
	protected static MapWritable degreeDist = new MapWritable();
	protected static HashMap<Integer,Float> degreeProb = new HashMap<Integer,Float>();
	
	protected static float adjustingFactorSeed=0.0f;
	protected static float relaxingFactorPropagation=0.0f;
	protected static boolean SAMPLING_ERROR = false;
	

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//SS0: CONVERTER PROPAGATE //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public static class ConverterPropagate
			extends AbstractComputation<IntWritable, VertexValue, EdgeValue, IntWritable, IntWritable> {

		@Override
		public void compute(Vertex<IntWritable, VertexValue, EdgeValue> vertex, Iterable<IntWritable> messages)
				throws IOException {
			aggregate(TOTAL_DIRECTED_OUT_EDGES, new LongWritable(vertex.getNumEdges()));
			vertex.getValue().setRealOutDegree(vertex.getNumEdges());
			sendMessageToAllEdges(vertex, vertex.getId());
			//RR:
			aggregate(AGG_VERTICES, new IntWritable(1));
		}
	}
	
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//SS1: CONVERTER UPDATE EDGES ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public static class ConverterUpdateEdges
			extends AbstractComputation<IntWritable, VertexValue, EdgeValue, IntWritable, SamplingMessage> {
		private byte edgeWeight;
		private String[] outDegreeFrequency;
		private int avgDegree;

		@Override
		public void compute(Vertex<IntWritable, VertexValue, EdgeValue> vertex, Iterable<IntWritable> messages)
				throws IOException {

			if(SAMPLING_TYPE.contentEquals("BGRAP")) {
				if (vertex.getNumEdges() >= 100) {
					aggregate(outDegreeFrequency[100], new IntWritable(1));
				} else {
					aggregate(outDegreeFrequency[vertex.getNumEdges()], new IntWritable(1));
				}
			}

			int inter = 0;
			for(IntWritable other : messages) {
				EdgeValue edgeValue = vertex.getEdgeValue(other);
				//if edge to other vertex doesn't exist, create a virtual one for this node
				if (edgeValue == null || edgeValue.isVirtualEdge()) {
					edgeValue = new EdgeValue();
					edgeValue.setWeight((byte) 1);
					edgeValue.setVirtualEdge(true);
					Edge<IntWritable, EdgeValue> edge = EdgeFactory.create(new IntWritable(other.get()), edgeValue);
					vertex.addEdge(edge);
					// aggregate(AGG_REAL_LOCAL_EDGES, new LongWritable(1));
				} else {
				//else change the edge value to a default one for edgecut calculations
					edgeValue = new EdgeValue();																		//RR: really necessary to create another edge? reuse the same! just comment and see result
					edgeValue.setWeight(edgeWeight);
					edgeValue.setVirtualEdge(false);
					vertex.setEdgeValue(other, edgeValue);
				}
				inter++;
			}
			//set real in-degree
			vertex.getValue().setRealInDegree(inter);
			
			//RR: calculate max degree
			aggregate(AGG_MAX_DEGREE, new IntWritable(vertex.getValue().getRealOutDegree()+vertex.getValue().getRealInDegree()));
			sendMessageToAllEdges(vertex, new SamplingMessage(vertex.getId().get(), -1)); //SEND MESSAGE TO KEEP ALIVE
		}

		// sets the edge-weight
		@Override
		public void preSuperstep() {
			
			edgeWeight = (byte) getContext().getConfiguration().getInt(EDGE_WEIGHT, DEFAULT_EDGE_WEIGHT);
			SAMPLING_TYPE = getContext().getConfiguration().get(SAMPLING_TYPE_HANDLE, DEFAULT_SAMPLING_TYPE);
			if(SAMPLING_TYPE.contentEquals("BGRAP")) {
				outDegreeFrequency = new String[101]; 															
				for (int i = 0; i < 101; i++) {
					outDegreeFrequency[i] = OUTDEGREE_FREQUENCY_COUNTER + i;
				}
			}
			
			//RR: change SIGMA percentage to long
			totalVertexNumber = ((IntWritable) getAggregatedValue(AGG_VERTICES)).get();
			BETA_P = getContext().getConfiguration().getFloat(BETA_HANDLE, (float) BETA_DEFAULT);
			BETA = ((int) (BETA_P*totalVertexNumber))+1;
			//BETA = (int) Math.ceil(Double.parseDouble(new Float(BETA_P*totalVertexNumber).toString()));
			SIGMA_P = getContext().getConfiguration().getFloat(SIGMA_HANDLE, (float) SIGMA_DEFAULT);
			SIGMA = ((int)(SIGMA_P*totalVertexNumber))+1;
			//SIGMA = (int) Math.ceil(Double.parseDouble(new Float(SIGMA_P*totalVertexNumber).toString()));
			TAU = getContext().getConfiguration().getInt(TAU_HANDLE, TAU_DEFAULT);
			//System.out.println("*Total Vertices:"+totalVertexNumber);
			//System.out.println("*BETA_P:"+BETA_P);
			//System.out.println("*SIGMA_P:"+SIGMA_P);
		}
	}

//SS2: POTENTIAL VERTICES INITIALIZER ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public static class PotentialVerticesInitializer
			extends AbstractComputation<IntWritable, VertexValue, EdgeValue, SamplingMessage, SamplingMessage> {
		protected String[] loadAggregatorNames;
		protected int numberOfPartitions;
		protected Random rnd = new Random();

		/**
		 * Store the current vertices-count of each partition
		 */
		protected String[] vertexCountAggregatorNames;
		protected long totalNumEdges;
		protected boolean directedGraph;
		private boolean debug;
		private int minOutDegreeThreshold;
		private int degreeThreshold;

		@Override
		public void compute(Vertex<IntWritable, VertexValue, EdgeValue> vertex, Iterable<SamplingMessage> messages)
				throws IOException {
			short partition = vertex.getValue().getCurrentPartition();
			// long numEdges = vertex.getNumEdges();
			long numOutEdges = vertex.getNumEdges();
			if (directedGraph) {
				numOutEdges = vertex.getValue().getRealOutDegree();
			}

			if ((partition == -1 && vertex.getValue().getRealOutDegree() > outDegreeThreshold)||partition == -2) { //RR: handle -2 exception to allow execution even when sampling has taken too many steps
				// initialize only hub vertices
				partition = (short) rnd.nextInt(numberOfPartitions);

				aggregate(loadAggregatorNames[partition], new LongWritable(numOutEdges));
				aggregate(vertexCountAggregatorNames[partition], new IntWritable(1));

				vertex.getValue().setCurrentPartition(partition);
				vertex.getValue().setNewPartition(partition);
				SamplingMessage message = new SamplingMessage(vertex.getId().get(), partition);
				sendMessageToAllEdges(vertex, message);

				aggregate(AGG_INITIALIZED_VERTICES, new IntWritable(1));
				aggregate(AGG_FIRST_LOADED_EDGES, new LongWritable(numOutEdges));
			}

			aggregate(AGG_UPPER_TOTAL_COMM_VOLUME, new LongWritable(Math.min(numberOfPartitions, numOutEdges)));
		}

		@Override
		public void preSuperstep() {
			directedGraph = getContext().getConfiguration().getBoolean(GRAPH_DIRECTED, DEFAULT_GRAPH_DIRECTED);

			totalNumEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();
			//totalVertexNumber = getTotalNumVertices(); //RR: changed it because the method is only the number of active vertices on previous SS
			totalVertexNumber = ((IntWritable) getAggregatedValue(AGG_VERTICES)).get(); 
			

			degreeThreshold = getContext().getConfiguration().getInt(OUTDEGREE_THRESHOLD, DEFAULT_OUTDEGREE_Threshold);
			minOutDegreeThreshold = getContext().getConfiguration().getInt(MIN_OUTDEGREE_THRESHOLD, DEFAULT_MIN_OUTDEGREE_Threshold);

			if (getContext().getConfiguration().getBoolean(COMPUTE_OUTDEGREE_THRESHOLD,
					DEFAULT_COMPUTE_OUTDEGREE_THRESHOLD)) {
				outDegreeThreshold = (int) Math.ceil((double) totalNumEdges / (double) totalVertexNumber);
			} else {
				outDegreeThreshold = degreeThreshold;
			}

			numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
			loadAggregatorNames = new String[numberOfPartitions];
			vertexCountAggregatorNames = new String[numberOfPartitions];
			for (int i = 0; i < numberOfPartitions; i++) {
				loadAggregatorNames[i] = AGG_EGDES_LOAD_PREFIX + i;
				vertexCountAggregatorNames[i] = AGG_VERTEX_COUNT_PREFIX + i;
			}
			debug = getContext().getConfiguration().getBoolean(DEBUG, false);
			//System.out.println("PreSuperstep Initializer : outDegreeThreshold=" + outDegreeThreshold);
			//aggregate(OUTDEGREE_THRESHOLD, new LongWritable(outDegreeThreshold));
		}
	}

//SS2: REPARTITIONER ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public static class Repartitioner
			extends AbstractComputation<IntWritable, VertexValue, EdgeValue, SamplingMessage, SamplingMessage> { //RR:Change to SamplingMessage
		private String[] loadAggregatorNames;
		private int numberOfPartitions;
		private short repartition;
		private double migrationProbability;

		/**
		 * Store the current vertices-count of each partition
		 */
		private String[] vertexCountAggregatorNames;
		private boolean directedGraph;
		private long totalNumEdges;

		@Override
		public void compute(Vertex<IntWritable, VertexValue, EdgeValue> vertex, Iterable<SamplingMessage> messages) //RR:Change to SamplingMessage
				throws IOException {
			short partition;
			short currentPartition = vertex.getValue().getCurrentPartition();

			long numEdges;
			if (directedGraph)
				numEdges = vertex.getValue().getRealOutDegree();
			else
				numEdges = vertex.getNumEdges();
			// down-scale
			if (repartition < 0) {
				if (currentPartition >= numberOfPartitions + repartition) {
					partition = (short) r.nextInt(numberOfPartitions + repartition);
				} else {
					partition = currentPartition;
				}
				// up-scale
			} else if (repartition > 0) {
				if (r.nextDouble() < migrationProbability) {
					partition = (short) (numberOfPartitions + r.nextInt(repartition));
				} else {
					partition = currentPartition;
				}
			} else {
				throw new RuntimeException("Repartitioner called with " + REPARTITION + " set to 0");
			}
			aggregate(loadAggregatorNames[partition], new LongWritable(numEdges));
			aggregate(vertexCountAggregatorNames[partition], new IntWritable(1));

			vertex.getValue().setCurrentPartition(partition);
			vertex.getValue().setNewPartition(partition);

			aggregate(AGG_UPPER_TOTAL_COMM_VOLUME,
					new LongWritable(Math.min(numberOfPartitions + repartition, numEdges)));

			SamplingMessage message = new SamplingMessage(vertex.getId().get(), partition);
			sendMessageToAllEdges(vertex, message);
		}

		@Override
		public void preSuperstep() {
			directedGraph = getContext().getConfiguration().getBoolean(GRAPH_DIRECTED, DEFAULT_GRAPH_DIRECTED);

			if (directedGraph)
				totalNumEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();
			else
				totalNumEdges = getTotalNumEdges();

			numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
			repartition = (short) getContext().getConfiguration().getInt(REPARTITION, DEFAULT_REPARTITION);
			migrationProbability = ((double) repartition) / (repartition + numberOfPartitions);
			loadAggregatorNames = new String[numberOfPartitions + repartition];
			vertexCountAggregatorNames = new String[numberOfPartitions + repartition];
			for (int i = 0; i < numberOfPartitions + repartition; i++) {
				loadAggregatorNames[i] = AGG_EGDES_LOAD_PREFIX + i;
				vertexCountAggregatorNames[i] = AGG_VERTEX_COUNT_PREFIX + i;
			}
		}
	}
	
	public static class ResetPartition
			// AbstractComputation<LongWritable, VertexValue, EdgeValue, LongWritable,
			// PartitionMessage>
			extends AbstractComputation<IntWritable, VertexValue, EdgeValue, IntWritable, SamplingMessage> {//RR:Change to SamplingMessage
		@Override
		public void compute(Vertex<IntWritable, VertexValue, EdgeValue> vertex, Iterable<IntWritable> messages)
				throws IOException {
			short partition = -1;

			vertex.getValue().setCurrentPartition(partition);
			vertex.getValue().setNewPartition(partition);

			SamplingMessage message = new SamplingMessage(vertex.getId().get(), partition); //RR:Change to SamplingMessage
			sendMessageToAllEdges(vertex, message);
		}
	}

	public static class ComputeNewPartition
			extends AbstractComputation<LongWritable, VertexValue, EdgeValue, SamplingMessage, LongWritable> {//RR:Change to SamplingMessage

		@Override
		public void compute(Vertex<LongWritable, VertexValue, EdgeValue> vertex, Iterable<SamplingMessage> messages)//RR:Change to SamplingMessage
				throws IOException {
			// TODO Auto-generated method stub

		}

	}

	public static class ComputeMigration
			extends AbstractComputation<LongWritable, VertexValue, EdgeValue, LongWritable, SamplingMessage> {//RR:Change to SamplingMessage

		@Override
		public void compute(Vertex<LongWritable, VertexValue, EdgeValue> vertex, Iterable<LongWritable> messages)
				throws IOException {
			// TODO Auto-generated method stub

		}
	}

	public static class ComputeGraphPartitionStatistics
			extends AbstractComputation<IntWritable, VertexValue, EdgeValue, SamplingMessage, IntWritable> {//RR:Change to SamplingMessage
		private byte edgeWeight;
		private boolean graphDirected;
		private byte newReverseEdgesWeight;

		private ShortArrayList maxIndices = new ShortArrayList();
		//
		private String[] demandAggregatorNames;
		private int[] partitionFrequency;
		/**
		 * connected partitions
		 */
		private ShortArrayList pConnect;

		/**
		 * Adnan: the balanced capacity of a partition |E|/K, |V|/K
		 */
		private long totalEdgeCapacity;
		private long totalVertexCapacity;
		private short totalNbPartitions;
		private double additionalCapacity;
		private long totalNumEdges;
		private boolean directedGraph;
		private String[] realLoadAggregatorNames;
		private String[] finalVertexCounts;
		private String[] virtualLoadAggregatorNames;
		private boolean debug;

		@Override
		public void compute(Vertex<IntWritable, VertexValue, EdgeValue> vertex, Iterable<SamplingMessage> messages)//RR:Change to SamplingMessage
				throws IOException {
			short p = vertex.getValue().getCurrentPartition();
			long realLocalEdges = 0, realInterEdges = 0, virtualLocalEdges = 0, virtualInterEdges = 0, realCV = 0;
			short p2;

			pConnect = new ShortArrayList();

			if (debug) {
				//System.out.println(vertex.getId() + "\t" + vertex.getValue().getRealOutDegree() + "\t"
				//		+ vertex.getValue().getRealInDegree());
			}

			// Adnan : update partition's vertices count
			aggregate(finalVertexCounts[p], new IntWritable(1));
			aggregate(realLoadAggregatorNames[p], new LongWritable(vertex.getValue().getRealOutDegree()));
			aggregate(virtualLoadAggregatorNames[p], new LongWritable(vertex.getNumEdges()));

			for (Edge<IntWritable, EdgeValue> e : vertex.getEdges()) {
				p2 = e.getValue().getPartition();

				if (debug) {
					//System.out.println(vertex.getId() + "-->" + e.getTargetVertexId() + "\t"
					//		+ e.getValue().isVirtualEdge() + "\t" + e.getValue().getWeight());
				}

				if (p2 == p) {
					if (!e.getValue().isVirtualEdge()) {
						realLocalEdges++;
					}
					virtualLocalEdges++;
				} else {
					if (!pConnect.contains(p2)) {
						pConnect.add(p2);
						if (!e.getValue().isVirtualEdge())
							realCV++;
					}
				}

			}
			realInterEdges = vertex.getValue().getRealOutDegree() - realLocalEdges;
			virtualInterEdges = vertex.getNumEdges() - virtualLocalEdges;
			// update cut edges stats
			aggregate(AGG_REAL_LOCAL_EDGES, new LongWritable(realLocalEdges));
			// ADNAN : update Total Comm Vol. State
			aggregate(AGG_REAL_EDGE_CUTS, new LongWritable(realInterEdges));
			// ADNAN : update Total Comm Vol. State
			aggregate(AGG_REAL_TOTAL_COMM_VOLUME, new LongWritable(realCV));

			aggregate(AGG_REAL_UPPER_TOTAL_COMM_VOLUME,
					new LongWritable(Math.min(totalNbPartitions, vertex.getValue().getRealOutDegree())));

			// update cut edges stats
			aggregate(AGG_VIRTUAL_LOCALS, new LongWritable(virtualLocalEdges));
			// ADNAN : update Total Comm Vol. State
			aggregate(AGG_VIRTUAL_EDGE_CUTS, new LongWritable(virtualInterEdges));
			// ADNAN : update Total Comm Vol. State
			aggregate(AGG_VIRTUAL_TOTAL_COMM_VOLUME, new LongWritable(pConnect.size()));

			aggregate(AGG_VIRTUAL_UPPER_TOTAL_COMM_VOLUME,
					new LongWritable(Math.min(totalNbPartitions, vertex.getNumEdges())));
		}

		@Override
		public void preSuperstep() {
			directedGraph = getContext().getConfiguration().getBoolean(GRAPH_DIRECTED, DEFAULT_GRAPH_DIRECTED);
			debug = getContext().getConfiguration().getBoolean(DEBUG, false);

			additionalCapacity = getContext().getConfiguration().getFloat(ADDITIONAL_CAPACITY,
					DEFAULT_ADDITIONAL_CAPACITY);
			totalNbPartitions = (short) (getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS)
					+ getContext().getConfiguration().getInt(REPARTITION, DEFAULT_REPARTITION));

			realLoadAggregatorNames = new String[totalNbPartitions];
			virtualLoadAggregatorNames = new String[totalNbPartitions];
			finalVertexCounts = new String[totalNbPartitions];
			for (int i = 0; i < totalNbPartitions; i++) {
				realLoadAggregatorNames[i] = AGG_REAL_LOAD_PREFIX + i;
				virtualLoadAggregatorNames[i] = AGG_VIRTUAL_LOAD_PREFIX + i;
				finalVertexCounts[i] = FINAL_AGG_VERTEX_COUNT_PREFIX + i;
			}
		}
	}

	/**
	 * The core of the partitioning algorithm executed on the Master
	 * @author Adnan
	 *
	 */
	public static class SuperPartitionerMasterCompute extends DefaultMasterCompute {
		protected LinkedList<Double> states;
		protected String[] loadAggregatorNames;
		protected int maxIterations;
		protected int numberOfPartitions;
		protected double convergenceThreshold;
		protected short repartition;
		protected int windowSize;
		protected double maxMinLoad;
		protected double maxNormLoad;
		protected double score;
		// Added by Adnan
		protected long totalMigrations;

		protected String[] realLoadAggregatorNames;
		protected String[] virtualLoadAggregatorNames;
		protected String[] vertexCountAggregatorNames;
		protected String[] finalVertexCounts;
		protected String[] outDegreeFrequency;

		protected long[] realLoads;
		protected long[] virtualLoads;
		protected int[] vCounts;
		protected double[] realAvgDegrees;
		protected double[] virtualAvgDegrees;

		protected double realMaxMinLoad;
		protected double realMaxNormLoad;
		protected double realEdgeBalanceJSD;

		protected double virtualMaxMinLoad;
		protected double virtualMaxNormLoad;
		protected double virtualEdgeBalanceJSD;

		protected double realMaxMinAvgDegree;
		protected double realMaxNormAvgDegree;
		protected double realAvgDegreeBalanceJSD;

		protected double virtualMaxMinAvgDegree;
		protected double virtualMaxNormAvgDegree;
		protected double virtualAvgDegreeBalanceJSD;

		protected double vertexMaxMinLoad;
		protected double vertexMaxNormLoad;
		protected double vertexBalanceJSD;
		
		//RR:
		protected String[] vertexCountAggregatorNamesSampling;
		
		/**
		 * if True, the statistics of the partition will be stored
		 * in a local file.
		 * (Please set the file name in saveXXX() methods)
		 */
		private boolean isSaveStatsIntoFile;

		@Override
		public void initialize() throws InstantiationException, IllegalAccessException {

		}
		
		/**
		 * Initialize the algorithm parameters (number of partitions, etc.)
		 * 
		 * Initialize the Aggregators (global/shared variables) needed to store 
		 * the state of the partition (edge cuts, number of edges/vertices, etc.).
		 * 
		 * @throws InstantiationException
		 * @throws IllegalAccessException
		 */
		protected void init() throws InstantiationException, IllegalAccessException {
			// DEFAULT_NUM_PARTITIONS = getConf().getMaxWorkers()*getConf().get();
			maxIterations = getContext().getConfiguration().getInt(MAX_ITERATIONS_LP, DEFAULT_MAX_ITERATIONS);

			numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);

			repartition = (short) getContext().getConfiguration().getInt(REPARTITION, DEFAULT_REPARTITION);

			isSaveStatsIntoFile = getContext().getConfiguration().getBoolean(SAVE_STATS, true);

			// Create aggregators for each partition
			realLoadAggregatorNames = new String[numberOfPartitions + repartition];
			virtualLoadAggregatorNames = new String[numberOfPartitions + repartition];
			vertexCountAggregatorNames = new String[numberOfPartitions + repartition];
			finalVertexCounts = new String[numberOfPartitions + repartition];

			for (int i = 0; i < numberOfPartitions + repartition; i++) {
				realLoadAggregatorNames[i] = AGG_REAL_LOAD_PREFIX + i;
				registerPersistentAggregator(realLoadAggregatorNames[i], LongSumAggregator.class);

				virtualLoadAggregatorNames[i] = AGG_VIRTUAL_LOAD_PREFIX + i;
				registerPersistentAggregator(virtualLoadAggregatorNames[i], LongSumAggregator.class);
				
				vertexCountAggregatorNames[i] = AGG_VERTEX_COUNT_PREFIX + i;
				registerPersistentAggregator(vertexCountAggregatorNames[i], LongSumAggregator.class);

				finalVertexCounts[i] = FINAL_AGG_VERTEX_COUNT_PREFIX + i;
				registerPersistentAggregator(finalVertexCounts[i], IntSumAggregator.class);
			}

			// Added by Adnan
			registerPersistentAggregator(AGG_REAL_UPPER_TOTAL_COMM_VOLUME, LongSumAggregator.class);
			registerPersistentAggregator(AGG_VIRTUAL_UPPER_TOTAL_COMM_VOLUME, LongSumAggregator.class);

			registerPersistentAggregator(TOTAL_DIRECTED_OUT_EDGES, LongSumAggregator.class);

			registerAggregator(AGG_REAL_TOTAL_COMM_VOLUME, LongSumAggregator.class);
			registerAggregator(AGG_VIRTUAL_TOTAL_COMM_VOLUME, LongSumAggregator.class);

			registerAggregator(AGG_REAL_EDGE_CUTS, LongSumAggregator.class);
			registerAggregator(AGG_VIRTUAL_EDGE_CUTS, LongSumAggregator.class);

			registerAggregator(AGG_REAL_LOCAL_EDGES, LongSumAggregator.class);
			registerAggregator(AGG_VIRTUAL_LOCALS, LongSumAggregator.class);

			if(SAMPLING_TYPE.contentEquals("BGRAP")) {
				outDegreeFrequency = new String[101];
				for (int i = 0; i < 101; i++) {
					outDegreeFrequency[i] = OUTDEGREE_FREQUENCY_COUNTER + i;
					registerPersistentAggregator(outDegreeFrequency[i], IntSumAggregator.class);
	
				}
			}
		}

		/**
		 * Print partition Stat. at a given superstep
		 * @param superstep superstep number
		 */
		protected void printStats(int superstep) {
			if (superstep > 2) {
				long migrations = ((LongWritable) getAggregatedValue(AGGREGATOR_MIGRATIONS)).get();
				long localEdges = ((LongWritable) getAggregatedValue(AGGREGATOR_LOCALS)).get();

				// Local edges
				// long realLocalEdges = ((LongWritable)
				// getAggregatedValue(AGG_REAL_LOCAL_EDGES)).get();
				// getContext().getCounter("current", "LE -
				// "+getSuperstep()).increment(realLocalEdges);

				switch (superstep % 2) {
				case 0:
					//System.out.println(((double) localEdges) / getTotalNumEdges() + " local edges");
					long minLoad = Long.MAX_VALUE;
					long maxLoad = -Long.MAX_VALUE;
					for (int i = 0; i < numberOfPartitions + repartition; i++) {
						long load = ((LongWritable) getAggregatedValue(loadAggregatorNames[i])).get();
						if (load < minLoad) {
							minLoad = load;
						}
						if (load > maxLoad) {
							maxLoad = load;
						}
					}
					double expectedLoad = ((double) getTotalNumEdges()) / (numberOfPartitions + repartition);
					//System.out.println((((double) maxLoad) / minLoad) + " max-min unbalance");
					//System.out.println(((maxLoad) / expectedLoad) + " maximum normalized load");
					break;
				case 1:
					//System.out.println(migrations + " migrations");
					break;
				}
			}
		}

		/**
		 * Update the partition Stat.
		 * @param superstep
		 */
		protected void updateStats() {
			totalMigrations += ((LongWritable) getAggregatedValue(AGGREGATOR_MIGRATIONS)).get();
			long minLoad = Long.MAX_VALUE;
			long maxLoad = -Long.MAX_VALUE;
			int k = numberOfPartitions + repartition;
			double expectedLoad = ((double) getTotalNumEdges()) / (k);

			double sumEdges = 0, sumVertex = 0;
			for (int i = 0; i < k; i++) {
				long load = ((LongWritable) getAggregatedValue(loadAggregatorNames[i])).get();
				// long compute = ((LongWritable)
				// getAggregatedValue(vertexCountAggregatorNames[i])).get();
				if (load < minLoad) {
					minLoad = load;
				}
				if (load > maxLoad) {
					maxLoad = load;
				}

				double loadProb = ((double) load) / getTotalNumEdges();
				sumEdges += -loadProb * Math.log(loadProb);

				// double computeProb = ((double) compute)/ getTotalNumVertices();
				// sumVertex += -computeProb * Math.log(computeProb);
			}

			maxMinLoad = ((double) maxLoad) / minLoad;
			maxNormLoad = (maxLoad) / expectedLoad;
			score = ((DoubleWritable) getAggregatedValue(AGGREGATOR_STATE)).get();
		}

		/**
		 * check if the algorithm has converged
		 * @param superstep the number of the current superstep
		 * @return
		 */
		protected boolean algorithmConverged(int superstep) {
			double newState = ((DoubleWritable) getAggregatedValue(AGGREGATOR_STATE)).get();
			boolean converged = false;
			if (superstep > sampling_ss_end + 3 + windowSize) {
				double best = Collections.max(states);
				double step = Math.abs(1 - newState / best);
				converged = step < convergenceThreshold;				
				states.removeFirst();
			}
			states.addLast(newState);

			return converged;
		}

		protected static boolean computeStates = false;
		protected static int lastStep = Integer.MAX_VALUE;

		@Override
		public void compute() {
		}

		/**
		 * Update the partition quality measures (evaluations measures)
		 */
		protected void updatePartitioningQuality() {
			int k = numberOfPartitions + repartition;

			long totalNumRealEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();

			long realMinLoad = Long.MAX_VALUE;
			long realMaxLoad = -Long.MAX_VALUE;
			double realExpectedLoad = ((double) totalNumRealEdges) / (k);

			long virtualMinLoad = Long.MAX_VALUE;
			long virtualMaxLoad = -Long.MAX_VALUE;
			double virtualExpectedLoad = ((double) getTotalNumEdges()) / (k);

			double realMinAvgDegree = Double.MAX_VALUE;
			double realMaxAvgDegree = -Double.MAX_VALUE;
			double realExpectedAvgDegree = ((double) totalNumRealEdges) / getTotalNumVertices();

			double virtualMinAvgDegree = Double.MAX_VALUE;
			double virtualMaxAvgDegree = -Double.MAX_VALUE;
			double virtualExpectedAvgDegree = ((double) getTotalNumEdges()) / getTotalNumVertices();

			int vertexMinLoad = Integer.MAX_VALUE;
			int vertexMaxLoad = -Integer.MAX_VALUE;
			double vertexExpectedLoad = ((double) getTotalNumVertices()) / (k);

			realEdgeBalanceJSD = 0;
			virtualEdgeBalanceJSD = 0;
			realAvgDegreeBalanceJSD = 0;
			virtualAvgDegreeBalanceJSD = 0;
			vertexBalanceJSD = 0;

			double u = ((double) 1 / k), m;

			double avgrealnorm = 0;
			double avgvirnorm = 0;
			realLoads = new long[k];
			virtualLoads = new long[k];
			vCounts = new int[k];
			realAvgDegrees = new double[k];
			virtualAvgDegrees = new double[k];

			for (int i = 0; i < k; i++) {

				realLoads[i] = ((LongWritable) getAggregatedValue(realLoadAggregatorNames[i])).get();
				virtualLoads[i] = ((LongWritable) getAggregatedValue(virtualLoadAggregatorNames[i])).get();
				vCounts[i] = ((IntWritable) getAggregatedValue(finalVertexCounts[i])).get();

				realAvgDegrees[i] = ((double) realLoads[i] / vCounts[i]);
				virtualAvgDegrees[i] = ((double) virtualLoads[i] / vCounts[i]);

				avgrealnorm += realAvgDegrees[i];
				avgvirnorm += virtualAvgDegrees[i];
			}

			for (int i = 0; i < k; i++) {

				if (realLoads[i] < realMinLoad) {
					realMinLoad = realLoads[i];
				}
				if (realLoads[i] > realMaxLoad) {
					realMaxLoad = realLoads[i];
				}

				if (virtualLoads[i] < virtualMinLoad) {
					virtualMinLoad = virtualLoads[i];
				}
				if (virtualLoads[i] > virtualMaxLoad) {
					virtualMaxLoad = virtualLoads[i];
				}

				// avg
				if (realAvgDegrees[i] < realMinAvgDegree) {
					realMinAvgDegree = realAvgDegrees[i];
				}
				if (realAvgDegrees[i] > realMaxAvgDegree) {
					realMaxAvgDegree = realAvgDegrees[i];
				}

				if (virtualAvgDegrees[i] < virtualMinAvgDegree) {
					virtualMinAvgDegree = virtualAvgDegrees[i];
				}
				if (virtualAvgDegrees[i] > virtualMaxAvgDegree) {
					virtualMaxAvgDegree = virtualAvgDegrees[i];
				}

				if (vCounts[i] < vertexMinLoad) {
					vertexMinLoad = vCounts[i];
				}
				if (vCounts[i] > vertexMaxLoad) {
					vertexMaxLoad = vCounts[i];
				}

				/*
				 * System.out.println("partition " + i); System.out.println("realAvgDegree " +
				 * realAvgDegree); System.out.println("virtualAvgDegree " + virtualAvgDegree);
				 * System.out.println("realLoad " + realLoad); System.out.println("virtualLoad "
				 * + virtualLoad); System.out.println("vertexCount " + vCount);
				 */

				double realLoadProb = ((double) realLoads[i]) / totalNumRealEdges;
				m = (realLoadProb + u) / 2;
				realEdgeBalanceJSD += realLoadProb * Math.log(realLoadProb / m) + u * Math.log(u / m);

				double virtualLoadProb = ((double) virtualLoads[i]) / getTotalNumEdges();
				m = (virtualLoadProb + u) / 2;
				virtualEdgeBalanceJSD += virtualLoadProb * Math.log(virtualLoadProb / m) + u * Math.log(u / m);

				// avg
				double realAvgDegreeProb = (realAvgDegrees[i]) / (avgrealnorm);
				m = (realAvgDegreeProb + u) / 2;
				realAvgDegreeBalanceJSD += realAvgDegreeProb * Math.log(realAvgDegreeProb / m) + u * Math.log(u / m);

				double virtualAvgDegreeLoadProb = (virtualAvgDegrees[i]) / (avgvirnorm);
				m = (virtualAvgDegreeLoadProb + u) / 2;
				virtualAvgDegreeBalanceJSD += virtualAvgDegreeLoadProb * Math.log(virtualAvgDegreeLoadProb / m)
						+ u * Math.log(u / m);

				// JensenShannonDivergence
				double vCountProb = ((double) vCounts[i]) / getTotalNumVertices();
				m = (vCountProb + u) / 2;
				vertexBalanceJSD += vCountProb * Math.log(vCountProb / m) + u * Math.log(u / m);
			}

			realMaxNormLoad = (realMaxLoad) / realExpectedLoad;
			realMaxMinLoad = ((double) realMaxLoad) / realMinLoad;
			realEdgeBalanceJSD = realEdgeBalanceJSD / (2 * Math.log(2));

			virtualMaxNormLoad = (virtualMaxLoad) / virtualExpectedLoad;
			virtualMaxMinLoad = ((double) virtualMaxLoad) / virtualMinLoad;
			virtualEdgeBalanceJSD = virtualEdgeBalanceJSD / (2 * Math.log(2));

			// avg
			realMaxNormAvgDegree = (realMaxAvgDegree) / realExpectedAvgDegree;
			realMaxMinAvgDegree = (realMaxAvgDegree) / realMinAvgDegree;
			realAvgDegreeBalanceJSD = realAvgDegreeBalanceJSD / (2 * Math.log(2));

			virtualMaxNormAvgDegree = (virtualMaxAvgDegree) / virtualExpectedAvgDegree;
			virtualMaxMinAvgDegree = (virtualMaxAvgDegree) / virtualMinAvgDegree;
			virtualAvgDegreeBalanceJSD = virtualAvgDegreeBalanceJSD / (2 * Math.log(2));

			vertexMaxNormLoad = (vertexMaxLoad) / vertexExpectedLoad;
			vertexMaxMinLoad = ((double) vertexMaxLoad) / vertexMinLoad;
			vertexBalanceJSD = vertexBalanceJSD / (2 * Math.log(2));


			System.out.println("Llegamos hasta aquí");
			System.out.println(isSaveStatsIntoFile);
			try {
				String filename = SAVE_PATH+formattedDate + "_" + SAMPLING_TYPE+".csv";
				
				System.out.println(filename);

				FileWriter file = new FileWriter(filename, true);
				savePartitionStats(file);
				saveRealStats(file);
				saveVirtualStats(file);
				file.flush();
				file.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


		@SuppressWarnings("unchecked")
		/**
		 * A method to store the computational time stat.
		 * 
		 * @param isSpinner a flag to distinguish between our Method 
		 * and Spinner method
		 * 
		 * @param totalMigrations
		 */
		
		protected void saveTimersStats(boolean isSpinner, long totalMigrations) {
			long firstLPIteration;
			long initializingLPTime;

			if (isSpinner) {// Spinner algo.
				if (repartition == 0)
					initializingLPTime = getContext().getCounter("Giraph Timers", "Superstep 2 Initializer (ms)")
							.getValue();
				else
					initializingLPTime = getContext().getCounter("Giraph Timers", "Superstep 2 Repartitioner (ms)")
							.getValue();
				firstLPIteration = getContext().getCounter("Giraph Timers", "Superstep 3 ComputeNewPartition (ms)")
						.getValue()
						+ getContext().getCounter("Giraph Timers", "Superstep 4 ComputeMigration (ms)").getValue();
			} else {// Our algo.
				if (repartition == 0)
					initializingLPTime = getContext()
							.getCounter("Giraph Timers", "Superstep 2 PotentialVerticesInitializer (ms)").getValue();
				else
					initializingLPTime = getContext().getCounter("Giraph Timers", "Superstep 2 Repartitioner (ms)")
							.getValue();

				firstLPIteration = getContext().getCounter("Giraph Timers", "Superstep 3 ComputeFirstPartition (ms)")
						.getValue()
						+ getContext().getCounter("Giraph Timers", "Superstep 4 ComputeFirstMigration (ms)").getValue();
			}

			long totalIterationsTime = firstLPIteration;
			float avg = 0;
			long totalLPSupersteps = getSuperstep() - 4;

			// compute the average superstep time
			for (int i = 5; i < getSuperstep() - 1; i = i + 2) {
				totalIterationsTime += getContext()
						.getCounter("Giraph Timers", "Superstep " + i + " ComputeNewPartition (ms)").getValue()
						+ getContext().getCounter("Giraph Timers", "Superstep " + (i + 1) + " ComputeMigration (ms)")
								.getValue();
			}
			avg = (float) totalIterationsTime / (totalLPSupersteps / 2);

			getContext().getCounter(PARTITION_COUNTER_GROUP, "Total LP (ms)").increment((totalIterationsTime));
			getContext().getCounter(PARTITION_COUNTER_GROUP, MIGRATIONS_COUNTER).increment(totalMigrations);
			getContext().getCounter(PARTITION_COUNTER_GROUP, SCORE_COUNTER).increment((long) (1000 * score));
			long totalRealEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();

			String date = new SimpleDateFormat("yyyy-MM-dd--hh:mm:ss").format(new Date());
			if (isSaveStatsIntoFile) {
				try {
					String filename = SAVE_PATH+formattedDate+"-TimeComparison.csv";
					FileWriter file = new FileWriter(filename, true);
					file.write(getContext().getJobID() + DELIMITER);
					file.write(date + DELIMITER);
					file.write(getContext().getJobName() + DELIMITER);
					file.write(getTotalNumVertices() + DELIMITER);
					file.write(getTotalNumEdges() + DELIMITER);
					file.write(totalRealEdges + DELIMITER);
					file.write(String.format("%.3f", ((float) getTotalNumEdges()) / totalRealEdges) + DELIMITER);
					file.write(getConf().getMaxWorkers() + DELIMITER);
					file.write(numberOfPartitions + repartition + DELIMITER);
					file.write((int) (totalLPSupersteps) / 2 + DELIMITER);
					file.write(initializingLPTime + DELIMITER);
					file.write(firstLPIteration + DELIMITER);
					file.write(totalIterationsTime + DELIMITER);
					file.write(getContext().getCounter("Giraph Timers", "Input superstep (ms)").getValue() + DELIMITER);
					file.write((long) avg + DELIMITER);
					file.write(totalMigrations + DELIMITER);
					file.write(getContext().getCounter("Giraph Stats", "Aggregate sent messages").getValue() + DELIMITER);
					file.write(getContext().getCounter(Task.Counter.VIRTUAL_MEMORY_BYTES).getValue() + DELIMITER);
					file.write(getContext().getCounter(Task.Counter.PHYSICAL_MEMORY_BYTES).getValue() + DELIMITER);
					file.write(getContext().getCounter(Task.Counter.COMMITTED_HEAP_BYTES).getValue() + DELIMITER);
					file.write(getContext().getCounter(Task.Counter.MAP_INPUT_BYTES).getValue() + DELIMITER);
					file.write(getContext().getCounter(Task.Counter.CPU_MILLISECONDS).getValue() + DELIMITER);				
					file.write(outDegreeThreshold + "\n");				
					file.flush();
					file.close();

					// rsync --partial --progress --rsh=ssh [source] [user]@[host]:[destination]
					//Runtime.getRuntime().exec("rsync --rsh=ssh " + filename + " adnanmoe@129.175.25.75:~/PaperResult/");

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				;
			}
		}
		

		/**
		 * A method to store the partition stat. based on real edges
		 */
		protected void saveRealStats(FileWriter file) {
			long totalRealEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();

			getContext().getCounter(PARTITION_COUNTER_GROUP, "R Maxmin unbalance (x1000)")
					.increment((long) (1000 * realMaxMinLoad));
			getContext().getCounter(PARTITION_COUNTER_GROUP, "R MaxNorm unbalance (x1000)")
					.increment((long) (1000 * realMaxNormLoad));

			// Local edges
			long realLocalEdges = ((LongWritable) getAggregatedValue(AGG_REAL_LOCAL_EDGES)).get();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#real LE").increment(realLocalEdges);
			double realLocalEdgesPct = ((double) realLocalEdges) / totalRealEdges;
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#real LE (%%)")
					.increment((long) (100 * realLocalEdgesPct));

			// Edge cuts
			long realEdgeCut = ((LongWritable) getAggregatedValue(AGG_REAL_EDGE_CUTS)).get();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#real EC").increment(realEdgeCut);
			double realEdgeCutPct = ((double) realEdgeCut) / totalRealEdges;
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#real EC (%%)").increment((long) (100 * realEdgeCutPct));

			// Communication volume
			long realComVolume = ((LongWritable) getAggregatedValue(AGG_REAL_TOTAL_COMM_VOLUME)).get();
			long realComVolumeNormalisationFactor = ((LongWritable) getAggregatedValue(
					AGG_REAL_UPPER_TOTAL_COMM_VOLUME)).get();
			double realComVolumePct = ((double) realComVolume) / realComVolumeNormalisationFactor;
			getContext().getCounter(PARTITION_COUNTER_GROUP, "real CV (%%)").increment((long) (100 * realComVolumePct));

			getContext().getCounter(PARTITION_COUNTER_GROUP, Vertex_Balance_JSD_COUNTER)
					.increment((long) (1000 * vertexBalanceJSD));

			getContext().getCounter(PARTITION_COUNTER_GROUP, Edge_Balance_JSD_COUNTER)
					.increment((long) (1000 * realEdgeBalanceJSD));

			if (isSaveStatsIntoFile) {
				try {
					file.write("REAL E MAXNORMLOAD"+DELIMITER+String.format("%.3f", realMaxNormLoad) +"\n");
					file.write("REAL E MAXMINLOAD"+DELIMITER+String.format("%.3f", realMaxMinLoad) +"\n");
					file.write("REAL EB JSD"+DELIMITER+String.format("%.6f", realEdgeBalanceJSD) +"\n");
					file.write("REAL E KLU"+DELIMITER+String.format("%.6f", divergenceKLU(realLoads)) +"\n");

					file.write("REAL V MAXNORMLOAD"+DELIMITER+String.format("%.3f", vertexMaxNormLoad) +"\n");
					file.write("REAL V MAXMINLOAD"+DELIMITER+String.format("%.3f", vertexMaxMinLoad) +"\n");
					file.write("REAL VB JSD"+DELIMITER+String.format("%.6f", vertexBalanceJSD) +"\n");
					file.write("REAL V KLU"+DELIMITER+String.format("%.6f", divergenceKLU(vCounts)) +"\n");

					file.write("REAL AVGD MAXNORM"+DELIMITER+String.format("%.3f", realMaxNormAvgDegree) +"\n");
					file.write("REAL AVGD MAXMIN"+DELIMITER+String.format("%.3f", realMaxMinAvgDegree) +"\n");
					file.write("REAL AVGDB JSD"+DELIMITER+String.format("%.6f", realAvgDegreeBalanceJSD) +"\n");
					file.write("REAL AVGD KLU"+DELIMITER+String.format("%.6f", divergenceKLU(realAvgDegrees)) +"\n");

					file.write("REAL EC PCT"+DELIMITER+String.format("%.3f", realEdgeCutPct) +"\n");
					file.write("REAL COMMVOL PCT"+DELIMITER+String.format("%.3f", realComVolumePct) +"\n");
					file.write("REAL LOCE PCT"+DELIMITER+String.format("%.3f", realLocalEdgesPct) +"\n");
					file.write("REAL EC"+DELIMITER+realEdgeCut +"\n");
					file.write("REAL COMMVOL"+DELIMITER+realComVolume +"\n");
					file.write("REAL LOCE"+DELIMITER+realLocalEdges +"\n");
					file.write("REAL COMMVOL NORMF"+DELIMITER+realComVolumeNormalisationFactor +"\n");
					file.write("REAL SCORE"+DELIMITER+score +"\n");
					//file.write("\n");

					// rsync --partial --progress --rsh=ssh [source] [user]@[host]:[destination]
					//Runtime.getRuntime().exec("rsync --rsh=ssh " + filename + " adnanmoe@129.175.25.75:~/PaperResult/");

				} catch (IOException e) { // TODO Auto-generated catch block
					e.printStackTrace();
				}
				;
			}
		}

		/**
		 * A method to store the partition stat. based on virtual edges
		 */
		protected void saveVirtualStats(FileWriter file) {
			long totalRealEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();

			getContext().getCounter(PARTITION_COUNTER_GROUP, "V Maxmin unbalance (x1000)")
					.increment((long) (1000 * virtualMaxMinLoad));
			getContext().getCounter(PARTITION_COUNTER_GROUP, "V MaxNorm unbalance (x1000)")
					.increment((long) (1000 * virtualMaxNormLoad));

			// Local edges
			long virtualLocalEdges = ((LongWritable) getAggregatedValue(AGG_VIRTUAL_LOCALS)).get();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#virtual local E").increment(virtualLocalEdges);
			double virtualLocalEdgesPct = ((double) virtualLocalEdges) / getTotalNumEdges();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "virtual local E (%%)")
					.increment((long) (100 * virtualLocalEdgesPct));

			// Edge cuts
			long virtualEdgeCut = ((LongWritable) getAggregatedValue(AGG_VIRTUAL_EDGE_CUTS)).get();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "#virtrual EC").increment(virtualEdgeCut);
			double virtualEdgeCutPct = ((double) virtualEdgeCut) / getTotalNumEdges();
			getContext().getCounter(PARTITION_COUNTER_GROUP, "virtrual EC (%%)")
					.increment((long) (100 * virtualEdgeCutPct));

			// Communication volume
			long virtualComVolume = ((LongWritable) getAggregatedValue(AGG_VIRTUAL_TOTAL_COMM_VOLUME)).get();
			long virtualComVolumeNormalisationFactor = ((LongWritable) getAggregatedValue(
					AGG_VIRTUAL_UPPER_TOTAL_COMM_VOLUME)).get();
			double virtualComVolumePct = ((double) virtualComVolume) / virtualComVolumeNormalisationFactor;
			getContext().getCounter(PARTITION_COUNTER_GROUP, "virtrual CV (%%)")
					.increment((long) (100 * virtualComVolumePct));

			getContext().getCounter(PARTITION_COUNTER_GROUP, Vertex_Balance_JSD_COUNTER)
					.increment((long) (1000 * vertexBalanceJSD));

			getContext().getCounter(PARTITION_COUNTER_GROUP, Edge_Balance_JSD_COUNTER)
					.increment((long) (1000 * virtualEdgeBalanceJSD));

			if (isSaveStatsIntoFile) {
				try {
					//file.write("JOB ID"+DELIMITER+getContext().getJobID() +"\n");
					//file.write("JOB NAME"+DELIMITER+getContext().getJobName() +"\n");
					//file.write("PARTITIONS"+DELIMITER+numberOfPartitions + repartition +"\n");
					file.write("VIRTUAL E MAXNORM LOAD"+DELIMITER+String.format("%.3f", virtualMaxNormLoad) +"\n");
					file.write("VIRTUAL E MAXMIN LOAD"+DELIMITER+String.format("%.3f", virtualMaxMinLoad) +"\n");
					file.write("VIRTUAL ED JSD"+DELIMITER+String.format("%.6f", virtualEdgeBalanceJSD) +"\n");
					file.write("VIRTUAL E KLU"+DELIMITER+String.format("%.6f", divergenceKLU(virtualLoads)) +"\n");

					file.write("VIRTUAL V MAXNORM LOAD"+DELIMITER+String.format("%.3f", vertexMaxNormLoad) +"\n");
					file.write("VIRTUAL V MAXMIN LOAD"+DELIMITER+String.format("%.3f", vertexMaxMinLoad) +"\n");
					file.write("VIRTUAL VB JSD"+DELIMITER+String.format("%.6f", vertexBalanceJSD) +"\n");
					file.write("VIRTUAL V KLU"+DELIMITER+String.format("%.6f", divergenceKLU(vCounts)) +"\n");

					file.write("VIRTUAL AVGD MAXNORM"+DELIMITER+String.format("%.3f", virtualMaxNormAvgDegree) +"\n");
					file.write("VIRTUAL AVGD MAXMIN"+DELIMITER+String.format("%.3f", virtualMaxMinAvgDegree) +"\n");
					file.write("VIRTUAL AVGDB JSD"+DELIMITER+String.format("%.6f", virtualAvgDegreeBalanceJSD) +"\n");
					file.write("VIRTUAL AVGD KLU"+DELIMITER+String.format("%.6f", divergenceKLU(virtualAvgDegrees)) +"\n");

					file.write("VIRTUAL EC PCT"+DELIMITER+String.format("%.3f", virtualEdgeCutPct) +"\n");
					file.write("VIRTUAL COMMVOL PCT"+DELIMITER+String.format("%.3f", virtualComVolumePct) +"\n");
					file.write("VIRTUAL LOCE PCT"+DELIMITER+String.format("%.3f", virtualLocalEdgesPct) +"\n");
					file.write("VIRTUAL EC"+DELIMITER+virtualEdgeCut +"\n");
					file.write("VIRTUAL COMMVOL"+DELIMITER+virtualComVolume +"\n");
					file.write("VIRTUAL LOCE"+DELIMITER+virtualLocalEdges +"\n");
					file.write("VIRTUAL COMMVOL NORMF"+DELIMITER+virtualComVolumeNormalisationFactor +"\n");
					//file.write("\n");

					// rsync --partial --progress --rsh=ssh [source] [user]@[host]:[destination]
					//Runtime.getRuntime().exec("rsync --rsh=ssh " + filename + " adnanmoe@129.175.25.75:~/PaperResult/");

				} catch (IOException e) { // TODO Auto-generated catch block
					e.printStackTrace();
				}
				;
			}
		}
		
		/**
		 * A method to store aditional stat.
		 */
		protected void savePartitionStats(FileWriter file) {
			if (isSaveStatsIntoFile) {
				int k = numberOfPartitions + repartition;
				int vertices = 0, edges = 0, vedges = 0;
				String delimiter_temp="_";
				String str1 = "", str2 = "", str3 = "";
				for (int i = 0; i < k; i++) {
					str1 += realLoads[i] + delimiter_temp;
					edges += realLoads[i];
					str2 += virtualLoads[i] + delimiter_temp;
					vedges += virtualLoads[i]; 
					str3 += vCounts[i] + delimiter_temp;
					vertices += vCounts[i];
				}

				try {
					//file.write(str[0] + "\n");
					file.write("JOB ID"+DELIMITER+getContext().getJobID() +"\n");
					file.write("JOB NAME"+DELIMITER+getContext().getJobName() +"\n");
					file.write("WORKERS"+DELIMITER+getConf().getMaxWorkers() +"\n");
					file.write("ALGORITHM"+DELIMITER+SAMPLING_TYPE+"\n");
					file.write("GRAPH"+DELIMITER+getContext().getConfiguration().get(GRAPH_NAME_HANDLE, GRAPH_NAME)+"\n");
					file.write("VERTICES"+DELIMITER+vertices+"\n");
					file.write("REAL EDGES"+DELIMITER+edges+"\n");
					file.write("VIRTUAL EDGES"+DELIMITER+vedges+"\n");
					file.write("PARITIONS"+DELIMITER+ k + "\n");
					file.write("REAL LOADS"+DELIMITER+str1 + "\n");
					file.write("VIRTUAL LOADS"+DELIMITER+str2 + "\n");
					file.write("VIRTUAL COUNTS"+DELIMITER+str3 + "\n");
					file.write("REAL KLU"+DELIMITER+(float) divergenceKLU(realLoads) + "\n");
					file.write("VIRTUAL KLU"+DELIMITER+(float) divergenceKLU(virtualLoads) + "\n");
					file.write("VIRTUAL COUNT KLU"+DELIMITER+(float) divergenceKLU(vCounts) + "\n");
					file.write("REAL AVGD KLU"+DELIMITER+(float) divergenceKLU(realAvgDegrees) + "\n");
					file.write("VIRTUAL AVGD KLU"+DELIMITER+(float) divergenceKLU(virtualAvgDegrees) + "\n");
					file.write("\n");

					// rsync --partial --progress --rsh=ssh [source] [user]@[host]:[destination]
					//Runtime.getRuntime().exec("rsync --rsh=ssh " + filename + " adnanmoe@129.175.25.75:~/PaperResult/");

				} catch (IOException e) { // TODO Auto-generated catch block
					e.printStackTrace();
				}
				;
			}
		}
		
		private double divergenceKLU(double[] p) {
			double distance = 0;
			double sum = sum(p);

			double u = ((double) 1) / (p.length);
			for (int i = 0; i < p.length; i++) {
				if (p[i] != 0) {
					double a = p[i] / sum;
					distance += a * Math.log(a / u);
				} else {
					return -1;
				}
			}
			return Math.abs(distance);
		}

		private double divergenceKLU(long[] p) {
			double distance = 0;
			long sum = sum(p);
			
			double u = ((double) 1) / (p.length);
			for (int i = 0; i < p.length; i++) {
				if (p[i] != 0) {
					double a = ((double) p[i]) / sum;
					distance += a * Math.log(a / u);
				} else {
					return -1;
				}
			}
			return distance / Math.log(p.length);
		}
		
		private double divergenceKLU(int[] p) {
			double distance = 0;
			int sum = sum(p);
			
			double u = ((double) 1) / (p.length);
			for (int i = 0; i < p.length; i++) {
				if (p[i] != 0) {
					double a = ((double) p[i]) / sum;
					distance += a * Math.log(a / u);
				} else {
					return -1;
				}
			}
			return distance / Math.log(p.length);
		}

		private double sum(double[] p) {
			double sum = 0;
			for (int i = 0; i < p.length; i++) {
				sum += p[i];
			}
			return sum;
		}
		
		private long sum(long[] p) {
			long sum = 0;
			for (int i = 0; i < p.length; i++) {
				sum += p[i];
			}
			return sum;
		}
		
		private int sum(int[] p) {
			int sum = 0;
			for (int i = 0; i < p.length; i++) {
				sum += p[i];
			}
			return sum;
		}
	}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RD: MASTER COMPUTE TEMP //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////	
	public static class RDVBMasterCompute extends SuperPartitionerMasterCompute {
		@Override
		public void initialize() throws InstantiationException, IllegalAccessException {
			maxIterations = getContext().getConfiguration().getInt(MAX_ITERATIONS_LP, DEFAULT_MAX_ITERATIONS);

			// DEFAULT_NUM_PARTITIONS = getConf().getMaxWorkers()*getConf().get();

			numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
			convergenceThreshold = getContext().getConfiguration().getFloat(CONVERGENCE_THRESHOLD,
					DEFAULT_CONVERGENCE_THRESHOLD);
			repartition = (short) getContext().getConfiguration().getInt(REPARTITION, DEFAULT_REPARTITION);
			windowSize = getContext().getConfiguration().getInt(WINDOW_SIZE, DEFAULT_WINDOW_SIZE);
			states = Lists.newLinkedList();
			// Create aggregators for each partition
			loadAggregatorNames = new String[numberOfPartitions + repartition];
			vertexCountAggregatorNames = new String[numberOfPartitions + repartition];
			vertexCountAggregatorNamesSampling = new String[numberOfPartitions + repartition]; //RR:
			
			for (int i = 0; i < numberOfPartitions + repartition; i++) {
				loadAggregatorNames[i] = AGG_EGDES_LOAD_PREFIX + i;
				registerPersistentAggregator(loadAggregatorNames[i], LongSumAggregator.class);
				registerAggregator(AGG_VERTEX_MIGRATION_DEMAND_PREFIX + i, IntSumAggregator.class); 

				vertexCountAggregatorNames[i] = AGG_VERTEX_COUNT_PREFIX + i;
				registerPersistentAggregator(vertexCountAggregatorNames[i], LongSumAggregator.class); // Hung
				
				//RR:
				vertexCountAggregatorNamesSampling[i] = AGG_VERTEX_COUNT_PREFIX + i +"_SAMPLING";
				registerAggregator(vertexCountAggregatorNamesSampling[i], LongSumAggregator.class);
				
			}
			registerAggregator(AGGREGATOR_STATE, DoubleSumAggregator.class); //RR: maybe float?
			registerAggregator(AGGREGATOR_LOCALS, LongSumAggregator.class);
			registerAggregator(AGGREGATOR_MIGRATIONS, LongSumAggregator.class); //RR: used for e and v, maybe create one for each and use int in V and long in E?

			// Added by Adnan
			registerAggregator(AGG_UPDATED_VERTICES, IntSumAggregator.class);
			registerAggregator(AGG_INITIALIZED_VERTICES, IntSumAggregator.class);
			registerAggregator(AGG_FIRST_LOADED_EDGES, LongSumAggregator.class);
			registerPersistentAggregator(AGG_UPPER_TOTAL_COMM_VOLUME, LongSumAggregator.class);
			registerAggregator(AGG_EDGE_CUTS, LongSumAggregator.class);
			
			//RR:
			SAMPLING_TYPE = getContext().getConfiguration().get(SAMPLING_TYPE_HANDLE, DEFAULT_SAMPLING_TYPE);
			SAVE_DD = getContext().getConfiguration().get(SAVE_DD_HANDLE, "false");
			
			registerAggregator(AGG_VERTICES,IntSumAggregator.class);
			registerPersistentAggregator(AGG_MAX_DEGREE, IntMaxAggregator.class);
			registerPersistentAggregator(AGG_SAMPLE, IntSumAggregator.class);
			registerAggregator(AGG_SAMPLE_SS,IntSumAggregator.class);
			registerAggregator(AGG_SAMPLE_SSR,IntSumAggregator.class);
			if (SAVE_DD.contentEquals("true") || SAMPLING_TYPE.contentEquals("InitializeSampleHD")|| SAMPLING_TYPE.contentEquals("InitializeSampleGD"))
				registerPersistentAggregator(AGG_DEGREE_DIST, HashMapAggregator.class);
			
			super.init();
			
			formattedDate = getContext().getConfiguration().get(DATE_HANDLE, "None");
			if(formattedDate == "None") {
				LocalDateTime myDateObj = LocalDateTime.now();
			    DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("MMdd_HHmmss");
			    formattedDate = myDateObj.format(myFormatObj);
			}
		}
		
		private static boolean computeStates = false;
		private static int lastStep = Integer.MAX_VALUE;
		
		@Override
		public void compute() {
			int superstep = (int) getSuperstep();
			if (computeStates) {
				if (superstep == lastStep + 1) {
					System.out.println("*MC"+superstep+": CGPS");
					setComputation(ComputeGraphPartitionStatistics.class); 
				} else {
					System.out.println("Finish stats.");
					haltComputation();
					updatePartitioningQuality();  												//RR: review datatypes
					saveTimersStats(totalMigrations);  											//RR: review datatypes
					saveDegreeDistribution(); 													//RR: check if it works with the hashmap
				}
			} else {
			switch(superstep){
				case 0:
					System.out.println("MC0: CP");
					setComputation(ConverterPropagate.class); 
					break;
				case 1:
					System.out.println("*MC1: CUE");
					setComputation(ConverterUpdateEdges.class);
					break;
				case 2:
					if (repartition != 0) {
						NEEDS_SAMPLE = false;
						setComputation(Repartitioner.class);									
					} else {
						sampling_messages_start = getContext().getCounter("Giraph Stats", "Aggregate sent messages").getValue();
						NEEDS_SAMPLE = true;
						if(BETA < getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS)) { 
							System.out.println("*WARNING: BETA is smaller than number of partitions wanted. Setting BETA to "+numberOfPartitions+".");
							BETA = numberOfPartitions;
						}
						//Set sampling execution
						System.out.println("*MC2: "+SAMPLING_TYPE);
						switch(SAMPLING_TYPE) {
						case "InitializeSampleRD":
							System.out.println("*SS"+superstep+":SAMPLING BETA"+BETA);
							System.out.println(":SAMPLING SIGMA"+SIGMA);
							System.out.println(":SAMPLING TAU"+TAU);
							setComputation(InitializeSampleRD.class);
							break;
						case "InitializeSampleHD":
							System.out.println("*SS"+superstep+":SAMPLING BETA"+BETA);
							System.out.println(":SAMPLING SIGMA"+SIGMA);
							System.out.println(":SAMPLING TAU"+TAU);
							setComputation(InitializeSampleHD.class);
							break;
						case "InitializeSampleGD":
							System.out.println("*SS"+superstep+":SAMPLING BETA"+BETA);
							System.out.println(":SAMPLING SIGMA"+SIGMA);
							setComputation(InitializeSampleGD.class);
							break;
						default:
							System.out.println("*WARNING: Type of algorithm not recognized. Executing BGRAP.");
							NEEDS_SAMPLE=false;
							sampling_ss_end = 2;
							setComputation(PotentialVerticesInitializer.class); 
							break;
						}
					}
					break;
				default:
					if (NEEDS_SAMPLE) {
						int sampleSize = ((IntWritable) getAggregatedValue(AGG_SAMPLE)).get();
						System.out.println("*MC"+superstep+": "+SAMPLING_TYPE+" SampleSize="+sampleSize);
						
						sampling_ss_end = (short) superstep;
						if(sampleSize>BETA) {
							sampling_ss_extra += 1;
						}
						
						if(superstep>150){ //RR: what is a good value for this condition?
							System.out.println("WARNING: Sampling did not execute correctly. Reverting to BGRAP.");
							NEEDS_SAMPLE = false;
							sampling_ss_end = (short) superstep;
							SAMPLING_ERROR = true;
							setComputation(PotentialVerticesInitializer.class);
						}else{
							switch(SAMPLING_TYPE) {
							case "InitializeSampleRD":
								setComputation(InitializeSampleRD.class);
								break;
							case "InitializeSampleHD":
								setComputation(InitializeSampleHD.class);
								break;
							case "InitializeSampleGD":
								setComputation(InitializeSampleGD.class);
								break;
							default:
								System.out.println("*WARNING: Type of algorithm not recognized. Executing BGRAP.");
								NEEDS_SAMPLE=false;
								sampling_ss_end = 2;
								setComputation(PotentialVerticesInitializer.class);
								break;
							}
						}
					} else {
						if (superstep == sampling_ss_end + 1) {
							sampling_messages_end = getContext().getCounter("Giraph Stats", "Aggregate sent messages").getValue();
							//System.out.println("*samplingStep: "+ sampling_ss_end);
							// System.out.println("after stp2 "+ getTotalNumEdges());
							getContext().getCounter("Partitioning Initialization", AGG_INITIALIZED_VERTICES)
									.increment(((IntWritable) getAggregatedValue(AGG_INITIALIZED_VERTICES)).get());
							//System.out.println("*MC"+superstep+": CFP");
							setComputation(BGRAP_vb.ComputeFirstPartition.class);											
						} else if (superstep == sampling_ss_end + 2) {

							getContext().getCounter("Partitioning Initialization", AGG_UPDATED_VERTICES)
									.increment(((IntWritable) getAggregatedValue(AGG_UPDATED_VERTICES)).get());
							//System.out.println("*MC"+superstep+": CFM");
							setComputation(BGRAP_vb.ComputeFirstMigration.class);											
						} else {
							switch ((superstep-sampling_ss_end) % 2) {
							case 0:
								//System.out.println("*MC"+superstep+": CM");
								setComputation(BGRAP_vb.ComputeMigration.class);
								break;
							case 1:
								//System.out.println("*MC"+superstep+": CNP");
								setComputation(BGRAP_vb.ComputeNewPartition.class);
								break;
							}
						}
					}
					break;
				}
				boolean hasConverged = false;
				if (superstep > sampling_ss_end + 3) {
					if ((superstep - sampling_ss_end) % 2 == 0) {
						hasConverged = algorithmConverged(superstep); 
					}
				}
				printStats(superstep); 
				updateStats(); 
				
				// LP iteration = 2 super-steps, LP process start after 3 super-steps 
				if (hasConverged || superstep >= (maxIterations*2+sampling_ss_end)) {
					lp_ss_end = (short) superstep;
					lp_messages_end = getContext().getCounter("Giraph Stats", "Aggregate sent messages").getValue();
					
					//System.out.println("Halting computation: " + hasConverged);
					computeStates = true;
					lastStep = superstep;
				}
			}
			
		}
		
		protected void saveTimersStats(long totalMigrations) {
			
			long initializingTime=0; //time to initialize nodes (+ time to make sure all partitions are initialized + time to initialize sampling)
			long samplingTime=0; //time to run RD cycles
			long firstLPIteration=0;
			long LPTime=0;
			long totalLPTime=0;
			
			long totalSamplingSupersteps=0; //RD cycle (request degree, receive degree, select sampled neighbors)
			long totalLPSupersteps=0;
			float avgSampling = 0;
			float avgLP = 0;
			
			if (repartition != 0) {
				initializingTime = getContext().getCounter("Giraph Timers", "Superstep 2 Repartitioner (ms)").getValue();
				firstLPIteration = getContext().getCounter("Giraph Timers", "Superstep 3 ComputeFirstPartition (ms)").getValue()
						+ getContext().getCounter("Giraph Timers", "Superstep 4 ComputeFirstMigration (ms)").getValue();
				LPTime = getLPTime(5);
				totalLPTime = firstLPIteration + LPTime;
				totalLPSupersteps = getSuperstep() - 4;
				avgLP = (float) totalLPTime / (totalLPSupersteps / 2);
			} else {
				switch (SAMPLING_TYPE) {
					case "InitializeSampleRD":
						initializingTime = getSamplingInitTime();
						samplingTime = getSamplingTime(2);
						totalSamplingSupersteps = sampling_ss_end - sampling_ss_extra - 1;
						avgSampling = (float) samplingTime / (totalSamplingSupersteps/3);
						break;
					
					case "InitializeSampleHD":
						initializingTime = getContext().getCounter("Giraph Timers", "Superstep 2 "+SAMPLING_TYPE+" (ms)").getValue() +
								+ getSamplingInitTime();
						samplingTime = getSamplingTime(3);
						totalSamplingSupersteps = sampling_ss_end - sampling_ss_extra - 2;
						avgSampling = (float) samplingTime / (totalSamplingSupersteps/3);
						break;
					case "InitializeSampleGD":
						initializingTime = getContext().getCounter("Giraph Timers", "Superstep 2 "+SAMPLING_TYPE+" (ms)").getValue() +
								+ getSamplingInitTime();
						samplingTime = getSamplingTime(3);
						totalSamplingSupersteps = sampling_ss_end - sampling_ss_extra - 2;
						avgSampling = (float) samplingTime / (totalSamplingSupersteps/3);
						break;
					default:
							System.out.println("WARNING: Unrecognized sampling type. Running BGRAP timers.");
							initializingTime = getContext().getCounter("Giraph Timers", "Superstep 2 PotentialVerticesInitializer (ms)").getValue();
						break;
				}

				firstLPIteration = getContext().getCounter("Giraph Timers", "Superstep "+(sampling_ss_end+1)+" ComputeFirstPartition (ms)").getValue()
						+ getContext().getCounter("Giraph Timers", "Superstep "+(sampling_ss_end+2)+" ComputeFirstMigration (ms)").getValue();
				LPTime = getLPTime(sampling_ss_end+3);
				totalLPTime = firstLPIteration + LPTime;
				totalLPSupersteps = getSuperstep() - (sampling_ss_end+2);
				avgLP = (float) totalLPTime / (totalLPSupersteps / 2);
			}
			
			getContext().getCounter(PARTITION_COUNTER_GROUP, "Total LP (ms)").increment((totalLPTime));
			getContext().getCounter(PARTITION_COUNTER_GROUP, MIGRATIONS_COUNTER).increment(totalMigrations);
			getContext().getCounter(PARTITION_COUNTER_GROUP, SCORE_COUNTER).increment((long) (1000 * score));
			long totalRealEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();

			String date = new SimpleDateFormat("yyyy-MM-dd--hh:mm:ss").format(new Date());
			if (super.isSaveStatsIntoFile) {
				try {
					String filename = SAVE_PATH+formattedDate+"_"+SAMPLING_TYPE+".csv";
					FileWriter file = new FileWriter(filename, true);
					file.write("\n");
					//SUPERSTEPS
					file.write("SAMPLING START"+DELIMITER+sampling_ss_start+"\n");
					file.write("SAMPLING END"+DELIMITER+sampling_ss_end+"\n");
					file.write("SAMPLING CYCLES"+DELIMITER+(int)(totalSamplingSupersteps)/3 +"\n");
					file.write("SAMPLING EXTRA"+DELIMITER+sampling_ss_extra+"\n");
					file.write("LP START"+DELIMITER+(sampling_ss_end+1)+"\n");
					file.write("LP END"+DELIMITER+lp_ss_end+"\n");
					file.write("LP CYCLES"+DELIMITER+(int)(totalLPSupersteps)/2+"\n");
					file.write("TOT MIGRATIONS"+DELIMITER+totalMigrations +"\n");
					file.write("\n");
					
					//MESSAGES
					long totalMessages = getContext().getCounter("Giraph Stats", "Aggregate sent messages").getValue();
					file.write("INITIAL MESSAGES"+DELIMITER+sampling_messages_start+"\n");
					file.write("SAMPLING MESSAGES"+DELIMITER+(sampling_messages_end-sampling_messages_start)+"\n");
					file.write("LP MESSAGES"+DELIMITER+(lp_messages_end-sampling_messages_end)+"\n");
					file.write("SHUTDOWN MESSAGES"+DELIMITER+(totalMessages-lp_messages_end)+"\n");
					file.write("TOTAL MESSAGES"+DELIMITER+totalMessages+"\n");
					file.write("\n");
					
					//TIME
					file.write("INPUT TIME (not included in total)"+DELIMITER+getContext().getCounter("Giraph Timers", "Input superstep (ms)").getValue() +"\n");
					file.write("INITIALIZE TIME"+DELIMITER+getContext().getCounter("Giraph Timers", "Initialize (ms)").getValue() +"\n");
					file.write("SETUP TIME"+DELIMITER+getContext().getCounter("Giraph Timers", "Setup (ms)").getValue() +"\n");
					//file.write("SHUTDOWN TIME"+DELIMITER+getContext().getCounter("Giraph Timers", "Shutdown (ms)").getValue()+"\n");
					//file.write("TOTAL TIME"+DELIMITER+getContext().getCounter("Giraph Timers", "Total (ms)").getValue()+"\n");
					
					file.write("Initializing algorithm time"+DELIMITER+initializingTime +"\n");
					//file.write("1LP TIME"+DELIMITER+firstLPIteration +"\n");
					//file.write("LP TIME"+DELIMITER+LPTime +"\n");
					file.write("LP time"+DELIMITER+totalLPTime +"\n");
					file.write("Avg. LP cycle time"+DELIMITER+(long) avgLP +"\n");
					file.write("Sampling time"+DELIMITER+samplingTime +"\n");
					file.write("Avg. Sampling cycle time"+DELIMITER+avgSampling +"\n");
					file.write("TOTAL TIME"+DELIMITER+(totalLPTime+samplingTime+initializingTime)+"\n");
					
					file.write("OUT DEGREE THRESHOLD"+DELIMITER+outDegreeThreshold + "\n");
					file.write("BETA"+DELIMITER+BETA_P +"\n");
					file.write("SIGMA"+DELIMITER+SIGMA_P+"\n");
					file.write("TAU"+DELIMITER+TAU+"\n");
					file.write("SAMPLING_ERROR"+DELIMITER+SAMPLING_ERROR+"\n");
				

					//MEMORY
					//file.write("VMEM BYTES"+DELIMITER+getContext().getCounter(Task.Counter.VIRTUAL_MEMORY_BYTES).getValue() +"\n");
					//file.write("PMEM BYTES"+DELIMITER+getContext().getCounter(Task.Counter.PHYSICAL_MEMORY_BYTES).getValue()+"\n");
					//file.write("HEAP BYTES"+DELIMITER+getContext().getCounter(Task.Counter.COMMITTED_HEAP_BYTES).getValue()+"\n");
					//file.write("MAP INPUT BYTES"+DELIMITER+getContext().getCounter(Task.Counter.MAP_INPUT_BYTES).getValue()+"\n");
					//file.write("CPU MS"+DELIMITER+getContext().getCounter(Task.Counter.CPU_MILLISECONDS).getValue()+"\n");
					
					//AE:
					file.flush();
					file.close();
					// rsync --partial --progress --rsh=ssh [source] [user]@[host]:[destination]
					//Runtime.getRuntime().exec("rsync --rsh=ssh " + filename + " adnanmoe@129.175.25.75:~/PaperResult/");

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		protected long getSamplingInitTime() {
			long SITime=0;
			for (int i = sampling_ss_end - sampling_ss_extra - 1; i <= sampling_ss_end; i++) {
				SITime += getContext().getCounter("Giraph Timers", "Superstep " + i + " "+SAMPLING_TYPE+" (ms)").getValue(); 
			}
			return SITime;
		}
		
		protected long getSamplingTime(int samplingStart) {
			long STime=0;
			for (int i = samplingStart; i < sampling_ss_end - sampling_ss_extra - 1; i++) {
				STime += getContext().getCounter("Giraph Timers", "Superstep " + i + " "+SAMPLING_TYPE+" (ms)").getValue();
			}
			return STime;
		}
		
		protected long getLPTime(int LPStart) {
			long LPTime=0;
			for (int i = LPStart; i < getSuperstep() - 1; i += 2) {
				LPTime += getContext().getCounter("Giraph Timers", "Superstep " + i + " ComputeNewPartition (ms)").getValue()
						+ getContext().getCounter("Giraph Timers", "Superstep " + (i + 1) + " ComputeMigration (ms)").getValue();
			}
			return LPTime;
		}
		
		protected void saveDegreeDistribution() {
			if (SAVE_DD.contentEquals("true")) {
				try {
					String graphName = getContext().getConfiguration().get(GRAPH_NAME_HANDLE, GRAPH_NAME);
					String filename = SAVE_PATH+graphName+"-GDD.csv";
					FileWriter file = new FileWriter(filename, true);
					
					for (Entry<Writable, Writable> entry : degreeDist.entrySet()) {
						file.write(((IntWritable)entry.getKey()).get()+DELIMITER+((IntWritable)entry.getValue()).get()+"\n");
					}

					//AE:
					file.flush();
					file.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//RD: MASTER COMPUTE TEMP //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////	
	public static class RDEBMasterCompute extends SuperPartitionerMasterCompute {
		@Override
		public void initialize() throws InstantiationException, IllegalAccessException {

			maxIterations = getContext().getConfiguration().getInt(MAX_ITERATIONS_LP, DEFAULT_MAX_ITERATIONS);

			// DEFAULT_NUM_PARTITIONS = getConf().getMaxWorkers()*getConf().get();

			numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
			convergenceThreshold = getContext().getConfiguration().getFloat(CONVERGENCE_THRESHOLD,
					DEFAULT_CONVERGENCE_THRESHOLD);
			repartition = (short) getContext().getConfiguration().getInt(REPARTITION, DEFAULT_REPARTITION);
			windowSize = getContext().getConfiguration().getInt(WINDOW_SIZE, DEFAULT_WINDOW_SIZE);
			states = Lists.newLinkedList();
			// Create aggregators for each partition
			loadAggregatorNames = new String[numberOfPartitions + repartition];
			vertexCountAggregatorNames = new String[numberOfPartitions + repartition];
			vertexCountAggregatorNamesSampling = new String[numberOfPartitions + repartition]; //RR:

			for (int i = 0; i < numberOfPartitions + repartition; i++) {
				loadAggregatorNames[i] = AGG_EGDES_LOAD_PREFIX + i;
				registerPersistentAggregator(loadAggregatorNames[i], LongSumAggregator.class);
				registerAggregator(AGG_VERTEX_MIGRATION_DEMAND_PREFIX + i, IntSumAggregator.class);
				registerAggregator(AGG_MIGRATION_DEMAND_PREFIX + i, LongSumAggregator.class); //added by Hung

				vertexCountAggregatorNames[i] = AGG_VERTEX_COUNT_PREFIX + i;
				registerPersistentAggregator(vertexCountAggregatorNames[i], LongSumAggregator.class); // Hung

				//RR:
				vertexCountAggregatorNamesSampling[i] = AGG_VERTEX_COUNT_PREFIX + i +"_SAMPLING";
				registerAggregator(vertexCountAggregatorNamesSampling[i], LongSumAggregator.class); // Hung

			}

			registerAggregator(AGGREGATOR_STATE, DoubleSumAggregator.class); //RR: maybe float?
			registerAggregator(AGGREGATOR_LOCALS, LongSumAggregator.class);
			registerAggregator(AGGREGATOR_MIGRATIONS, LongSumAggregator.class); //RR: used for e and v, maybe create one for each and use int in V and long in E?

			// Added by Adnan
			registerAggregator(AGG_UPDATED_VERTICES, LongSumAggregator.class); // Hung
			registerAggregator(AGG_INITIALIZED_VERTICES, IntSumAggregator.class);
			registerAggregator(AGG_FIRST_LOADED_EDGES, LongSumAggregator.class);
			registerPersistentAggregator(AGG_UPPER_TOTAL_COMM_VOLUME, LongSumAggregator.class);
			registerPersistentAggregator(TOTAL_DIRECTED_OUT_EDGES, LongSumAggregator.class);
			registerAggregator(AGG_EDGE_CUTS, LongSumAggregator.class);

			//RR:
			SAMPLING_TYPE = getContext().getConfiguration().get(SAMPLING_TYPE_HANDLE, DEFAULT_SAMPLING_TYPE);
			SAVE_DD = getContext().getConfiguration().get(SAVE_DD_HANDLE, "false");

			registerAggregator(AGG_VERTICES,IntSumAggregator.class);
			registerPersistentAggregator(AGG_MAX_DEGREE, IntMaxAggregator.class);
			registerPersistentAggregator(AGG_SAMPLE, IntSumAggregator.class);
			registerAggregator(AGG_SAMPLE_SS,IntSumAggregator.class);
			registerAggregator(AGG_SAMPLE_SSR,IntSumAggregator.class);
			if (SAVE_DD.contentEquals("true") || SAMPLING_TYPE.contentEquals("InitializeSampleHD")|| SAMPLING_TYPE.contentEquals("InitializeSampleGD"))
				registerPersistentAggregator(AGG_DEGREE_DIST, HashMapAggregator.class);

			super.init();

			formattedDate = getContext().getConfiguration().get(DATE_HANDLE, "None");
			if(formattedDate == "None") {
				LocalDateTime myDateObj = LocalDateTime.now();
				DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("MMdd_HHmmss");
				formattedDate = myDateObj.format(myFormatObj);
			}
		}

		private static boolean computeStates = false;
		private static int lastStep = Integer.MAX_VALUE;

		@Override
		public void compute() {
			System.out.println("Start the machine");
			int superstep = (int) getSuperstep();
			if (computeStates) {
				if (superstep == lastStep + 1) {
					System.out.println("*MC"+superstep+": CGPS");
					setComputation(ComputeGraphPartitionStatistics.class); 
				} else {
					System.out.println("Finish stats.");
					haltComputation();
					updatePartitioningQuality();  												//RR: review datatypes
					saveTimersStats(totalMigrations);  											//RR: review datatypes
					saveDegreeDistribution(); 													//RR: check if it works with the hashmap
				}
			} else {
				switch(superstep){
				case 0:
					System.out.println("MC0: CP");
					setComputation(ConverterPropagate.class); 
					break;
				case 1:
					System.out.println("*MC1: CUE");
					setComputation(ConverterUpdateEdges.class);
					break;
				case 2:
					if (repartition != 0) {
						NEEDS_SAMPLE = false;
						setComputation(Repartitioner.class);									
					} else {
						sampling_messages_start = getContext().getCounter("Giraph Stats", "Aggregate sent messages").getValue();
						NEEDS_SAMPLE = true;
						if(BETA < getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS)) { 
							System.out.println("*WARNING: BETA is smaller than number of partitions wanted. Setting BETA to "+numberOfPartitions+".");
							BETA = numberOfPartitions;
						}
						//Set sampling execution
						System.out.println("*MC2: "+SAMPLING_TYPE);
						switch(SAMPLING_TYPE) {
						case "InitializeSampleRD":
							System.out.println("*SS"+superstep+":SAMPLING BETA"+BETA);
							System.out.println(":SAMPLING SIGMA"+SIGMA);
							System.out.println(":SAMPLING TAU"+TAU);
							setComputation(InitializeSampleRD.class);
							break;
						case "InitializeSampleHD":
							System.out.println("*SS"+superstep+":SAMPLING BETA"+BETA);
							System.out.println(":SAMPLING SIGMA"+SIGMA);
							System.out.println(":SAMPLING TAU"+TAU);
							setComputation(InitializeSampleHD.class);
							break;
						case "InitializeSampleGD":
							System.out.println("*SS"+superstep+":SAMPLING BETA"+BETA);
							System.out.println(":SAMPLING SIGMA"+SIGMA);
							setComputation(InitializeSampleGD.class);
							break;
						default:
							System.out.println("*WARNING: Type of algorithm not recognized. Executing BGRAP.");
							NEEDS_SAMPLE=false;
							sampling_ss_end = 2;
							setComputation(PotentialVerticesInitializer.class); 
							break;
						}
					}
					break;
				default:
					if (NEEDS_SAMPLE) {
						int sampleSize = ((IntWritable) getAggregatedValue(AGG_SAMPLE)).get();
						System.out.println("*MC"+superstep+": "+SAMPLING_TYPE+" SampleSize="+sampleSize);

						sampling_ss_end = (short) superstep;
						if(sampleSize>BETA) {
							sampling_ss_extra += 1;
						}

						if(superstep>150){ //RR: what is a good value for this condition?
							System.out.println("WARNING: Sampling did not execute correctly. Reverting to BGRAP.");
							NEEDS_SAMPLE = false;
							sampling_ss_end = (short) superstep;
							SAMPLING_ERROR = true;
							setComputation(PotentialVerticesInitializer.class);
						}else{
							switch(SAMPLING_TYPE) {
							case "InitializeSampleRD":
								setComputation(InitializeSampleRD.class);
								break;
							case "InitializeSampleHD":
								setComputation(InitializeSampleHD.class);
								break;
							case "InitializeSampleGD":
								setComputation(InitializeSampleGD.class);
								break;
							default:
								System.out.println("*WARNING: Type of algorithm not recognized. Executing BGRAP.");
								NEEDS_SAMPLE=false;
								sampling_ss_end = 2;
								setComputation(PotentialVerticesInitializer.class);
								break;
							}
						}
					} else {
						if (superstep == sampling_ss_end + 1) {
							sampling_messages_end = getContext().getCounter("Giraph Stats", "Aggregate sent messages").getValue();
							//System.out.println("*samplingStep: "+ sampling_ss_end);
							// System.out.println("after stp2 "+ getTotalNumEdges());
							getContext().getCounter("Partitioning Initialization", AGG_INITIALIZED_VERTICES)
							.increment(((IntWritable) getAggregatedValue(AGG_INITIALIZED_VERTICES)).get());
							//System.out.println("*MC"+superstep+": CFP");
							setComputation(BGRAP_eb.ComputeFirstPartition.class);											
						} else if (superstep == sampling_ss_end + 2) {

							getContext().getCounter("Partitioning Initialization", AGG_UPDATED_VERTICES)
							.increment(((LongWritable) getAggregatedValue(AGG_UPDATED_VERTICES)).get()); //Hung
							//System.out.println("*MC"+superstep+": CFM");
							setComputation(BGRAP_eb.ComputeFirstMigration.class);											
						} else {
							switch ((superstep-sampling_ss_end) % 2) {
							case 0:
								//System.out.println("*MC"+superstep+": CM");
								setComputation(BGRAP_eb.ComputeMigration.class);
								break;
							case 1:
								//System.out.println("*MC"+superstep+": CNP");
								setComputation(BGRAP_eb.ComputeNewPartition.class);
								break;
							}
						}
					}
					break;
				}
				boolean hasConverged = false;
				if (superstep > sampling_ss_end + 3) {
					if ((superstep - sampling_ss_end) % 2 == 0) {
						hasConverged = algorithmConverged(superstep); 
					}
				}
				printStats(superstep); 
				updateStats(); 

				// LP iteration = 2 super-steps, LP process start after 3 super-steps 
				if (hasConverged || superstep >= (maxIterations*2+sampling_ss_end)) {
					lp_ss_end = (short) superstep;
					lp_messages_end = getContext().getCounter("Giraph Stats", "Aggregate sent messages").getValue();

					System.out.println("Halting computation: " + hasConverged);
					computeStates = true;
					lastStep = superstep;
				}
			}

		}

		protected void saveTimersStats(long totalMigrations) {

			long initializingTime=0; //time to initialize nodes (+ time to make sure all partitions are initialized + time to initialize sampling)
			long samplingTime=0; //time to run RD cycles
			long firstLPIteration=0;
			long LPTime=0;
			long totalLPTime=0;

			long totalSamplingSupersteps=0; //RD cycle (request degree, receive degree, select sampled neighbors)
			long totalLPSupersteps=0;
			float avgSampling = 0;
			float avgLP = 0;

			if (repartition != 0) {
				initializingTime = getContext().getCounter("Giraph Timers", "Superstep 2 Repartitioner (ms)").getValue();
				firstLPIteration = getContext().getCounter("Giraph Timers", "Superstep 3 ComputeFirstPartition (ms)").getValue()
						+ getContext().getCounter("Giraph Timers", "Superstep 4 ComputeFirstMigration (ms)").getValue();
				LPTime = getLPTime(5);
				totalLPTime = firstLPIteration + LPTime;
				totalLPSupersteps = getSuperstep() - 4;
				avgLP = (float) totalLPTime / (totalLPSupersteps / 2);
			} else {
				switch (SAMPLING_TYPE) {
				case "InitializeSampleRD":
					initializingTime = getSamplingInitTime();
					samplingTime = getSamplingTime(2);
					totalSamplingSupersteps = sampling_ss_end - sampling_ss_extra - 1;
					avgSampling = (float) samplingTime / (totalSamplingSupersteps/3);
					break;

				case "InitializeSampleHD":
					initializingTime = getContext().getCounter("Giraph Timers", "Superstep 2 "+SAMPLING_TYPE+" (ms)").getValue() +
					+ getSamplingInitTime();
					samplingTime = getSamplingTime(3);
					totalSamplingSupersteps = sampling_ss_end - sampling_ss_extra - 2;
					avgSampling = (float) samplingTime / (totalSamplingSupersteps/3);
					break;
				case "InitializeSampleGD":
					initializingTime = getContext().getCounter("Giraph Timers", "Superstep 2 "+SAMPLING_TYPE+" (ms)").getValue() +
					+ getSamplingInitTime();
					samplingTime = getSamplingTime(3);
					totalSamplingSupersteps = sampling_ss_end - sampling_ss_extra - 2;
					avgSampling = (float) samplingTime / (totalSamplingSupersteps/3);
					break;
				default:
					System.out.println("WARNING: Unrecognized sampling type. Running BGRAP timers.");
					initializingTime = getContext().getCounter("Giraph Timers", "Superstep 2 PotentialVerticesInitializer (ms)").getValue();
					break;
				}

				firstLPIteration = getContext().getCounter("Giraph Timers", "Superstep "+(sampling_ss_end+1)+" ComputeFirstPartition (ms)").getValue()
						+ getContext().getCounter("Giraph Timers", "Superstep "+(sampling_ss_end+2)+" ComputeFirstMigration (ms)").getValue();
				LPTime = getLPTime(sampling_ss_end+3);
				totalLPTime = firstLPIteration + LPTime;
				totalLPSupersteps = getSuperstep() - (sampling_ss_end+2);
				avgLP = (float) totalLPTime / (totalLPSupersteps / 2);
			}

			getContext().getCounter(PARTITION_COUNTER_GROUP, "Total LP (ms)").increment((totalLPTime));
			getContext().getCounter(PARTITION_COUNTER_GROUP, MIGRATIONS_COUNTER).increment(totalMigrations);
			getContext().getCounter(PARTITION_COUNTER_GROUP, SCORE_COUNTER).increment((long) (1000 * score));
			long totalRealEdges = ((LongWritable) getAggregatedValue(TOTAL_DIRECTED_OUT_EDGES)).get();

			String date = new SimpleDateFormat("yyyy-MM-dd--hh:mm:ss").format(new Date());
			if (super.isSaveStatsIntoFile) {
				try {
					String filename = SAVE_PATH+formattedDate+"_"+SAMPLING_TYPE+".csv";
					FileWriter file = new FileWriter(filename, true);
					file.write("\n");
					//SUPERSTEPS
					file.write("SAMPLING START"+DELIMITER+sampling_ss_start+"\n");
					file.write("SAMPLING END"+DELIMITER+sampling_ss_end+"\n");
					file.write("SAMPLING CYCLES"+DELIMITER+(int)(totalSamplingSupersteps)/3 +"\n");
					file.write("SAMPLING EXTRA"+DELIMITER+sampling_ss_extra+"\n");
					file.write("LP START"+DELIMITER+(sampling_ss_end+1)+"\n");
					file.write("LP END"+DELIMITER+lp_ss_end+"\n");
					file.write("LP CYCLES"+DELIMITER+(int)(totalLPSupersteps)/2+"\n");
					file.write("TOT MIGRATIONS"+DELIMITER+totalMigrations +"\n");
					file.write("\n");

					//MESSAGES
					long totalMessages = getContext().getCounter("Giraph Stats", "Aggregate sent messages").getValue();
					file.write("INITIAL MESSAGES"+DELIMITER+sampling_messages_start+"\n");
					file.write("SAMPLING MESSAGES"+DELIMITER+(sampling_messages_end-sampling_messages_start)+"\n");
					file.write("LP MESSAGES"+DELIMITER+(lp_messages_end-sampling_messages_end)+"\n");
					file.write("SHUTDOWN MESSAGES"+DELIMITER+(totalMessages-lp_messages_end)+"\n");
					file.write("TOTAL MESSAGES"+DELIMITER+totalMessages+"\n");
					file.write("\n");

					//TIME
					file.write("INPUT TIME (not included in total)"+DELIMITER+getContext().getCounter("Giraph Timers", "Input superstep (ms)").getValue() +"\n");
					file.write("INITIALIZE TIME"+DELIMITER+getContext().getCounter("Giraph Timers", "Initialize (ms)").getValue() +"\n");
					file.write("SETUP TIME"+DELIMITER+getContext().getCounter("Giraph Timers", "Setup (ms)").getValue() +"\n");
					//file.write("SHUTDOWN TIME"+DELIMITER+getContext().getCounter("Giraph Timers", "Shutdown (ms)").getValue()+"\n");
					//file.write("TOTAL TIME"+DELIMITER+getContext().getCounter("Giraph Timers", "Total (ms)").getValue()+"\n");

					file.write("Initializing algorithm time"+DELIMITER+initializingTime +"\n");
					//file.write("1LP TIME"+DELIMITER+firstLPIteration +"\n");
					//file.write("LP TIME"+DELIMITER+LPTime +"\n");
					file.write("LP time"+DELIMITER+totalLPTime +"\n");
					file.write("Avg. LP cycle time"+DELIMITER+(long) avgLP +"\n");
					file.write("Sampling time"+DELIMITER+samplingTime +"\n");
					file.write("Avg. Sampling cycle time"+DELIMITER+avgSampling +"\n");
					file.write("TOTAL TIME"+DELIMITER+(totalLPTime+samplingTime+initializingTime)+"\n");

					file.write("OUT DEGREE THRESHOLD"+DELIMITER+outDegreeThreshold + "\n");
					file.write("BETA"+DELIMITER+BETA_P +"\n");
					file.write("SIGMA"+DELIMITER+SIGMA_P+"\n");
					file.write("TAU"+DELIMITER+TAU+"\n");
					file.write("SAMPLING_ERROR"+DELIMITER+SAMPLING_ERROR+"\n");

					//MEMORY
					//file.write("VMEM BYTES"+DELIMITER+getContext().getCounter(Task.Counter.VIRTUAL_MEMORY_BYTES).getValue() +"\n");
					//file.write("PMEM BYTES"+DELIMITER+getContext().getCounter(Task.Counter.PHYSICAL_MEMORY_BYTES).getValue()+"\n");
					//file.write("HEAP BYTES"+DELIMITER+getContext().getCounter(Task.Counter.COMMITTED_HEAP_BYTES).getValue()+"\n");
					//file.write("MAP INPUT BYTES"+DELIMITER+getContext().getCounter(Task.Counter.MAP_INPUT_BYTES).getValue()+"\n");
					//file.write("CPU MS"+DELIMITER+getContext().getCounter(Task.Counter.CPU_MILLISECONDS).getValue()+"\n");

					//AE:
					file.flush();
					file.close();
					// rsync --partial --progress --rsh=ssh [source] [user]@[host]:[destination]
					//Runtime.getRuntime().exec("rsync --rsh=ssh " + filename + " adnanmoe@129.175.25.75:~/PaperResult/");

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		protected long getSamplingInitTime() {
			long SITime=0;
			for (int i = sampling_ss_end - sampling_ss_extra - 1; i <= sampling_ss_end; i++) {
				SITime += getContext().getCounter("Giraph Timers", "Superstep " + i + " "+SAMPLING_TYPE+" (ms)").getValue(); 
			}
			return SITime;
		}

		protected long getSamplingTime(int samplingStart) {
			long STime=0;
			for (int i = samplingStart; i < sampling_ss_end - sampling_ss_extra - 1; i++) {
				STime += getContext().getCounter("Giraph Timers", "Superstep " + i + " "+SAMPLING_TYPE+" (ms)").getValue();
			}
			return STime;
		}

		protected long getLPTime(int LPStart) {
			long LPTime=0;
			for (int i = LPStart; i < getSuperstep() - 1; i += 2) {
				LPTime += getContext().getCounter("Giraph Timers", "Superstep " + i + " ComputeNewPartition (ms)").getValue()
						+ getContext().getCounter("Giraph Timers", "Superstep " + (i + 1) + " ComputeMigration (ms)").getValue();
			}
			return LPTime;
		}

		protected void saveDegreeDistribution() {
			if (SAVE_DD.contentEquals("true")) {
				try {
					String graphName = getContext().getConfiguration().get(GRAPH_NAME_HANDLE, GRAPH_NAME);
					String filename = SAVE_PATH+graphName+"-GDD.csv";
					FileWriter file = new FileWriter(filename, true);

					for (Entry<Writable, Writable> entry : degreeDist.entrySet()) {
						file.write(((IntWritable)entry.getKey()).get()+DELIMITER+((IntWritable)entry.getValue()).get()+"\n");
					}

					//AE:
					file.flush();
					file.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
