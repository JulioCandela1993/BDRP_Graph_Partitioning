date := $(shell date +'%m%d_%H%M%S')
code := RankDegree.jar
sampling := 'InitializeSampleHD'
partitions := 8
#graph := c_graph_30_0.3_0.2_5
#graph := WikiTalk
#graph := BerkeleyStan
#graph := Flixster
#graph := DelaunaySC --no
#graph := Pokec --no
#graph := soc-LiveJournal
#graph := Orkut
#graph := graph500-scale23-ef16
graph := Twitter
#graph := sk-2005
w := 16
main := 'giraph.lri.rrojas.rankdegree.LPGPartitionner$$ConverterPropagate'
masterCompute := 'giraph.lri.rrojas.rankdegree.LPGPartitionner$$RDVBMasterCompute'
vif := giraph.format.personalized.BGRAPVertexInputFormat
vof := giraph.format.personalized.BGRAP_IdWithPartitionOuputFormat
vifo := giraph.lri.rrojas.rankdegree.RR_BGRAP_InputFormat
vofo := giraph.lri.rrojas.rankdegree.RR_BGRAP_OuputFormat
ip := /rrojas/inputdata/$(graph)
op := /rrojas/outputdata/$(date)_$(sampling)_$(graph)_$(w)_folder
beta := 0.10 #0.4
sigma := 0.01 #0.05
tau := 5
save_dd := true

create_dirs:
        hadoop fs -mkdir /rrojas/
        hadoop fs -mkdir /rrojas/inputdata/
        hadoop fs -mkdir /rrojas/outputdata/

ls_code:
        ls -l $(HADOOP_HOME)/lib/R*
hls:
        hadoop fs -lsr /
put_data:
        hadoop fs -put ~/testdata/$(graph) /rrojas/inputdata/.
put_all_data:
        hadoop fs -put ~/testdata/* /rrojas/inputdata/.
rm_data:
        hadoop fs -rm /rrojas/inputdata/$(graph)
get_results:
        rm -fR ~/Results/*
        hadoop fs -get /rrojas/outputdata/* ~/Results
rm_logs:
        rm -fR $(HADOOP_LOGS)/*
rm_results:
        rm -fR ~/Results/*
        rm -fR ~/PaperResult/*
hrm_results:
        hadoop fs -rmr  /rrojas/outputdata/*
clean_container:
        hadoop fs -rmr /user
        hadoop fs -rmr /tmp

clean_all:
        make rm_logs
        make rm_results
        make hrm_results
        make clean_container

run:
        giraph $(HADOOP_HOME)/lib/$(code) $(main) -w $(w) -vif $(vif) -vip $(ip) -vof $(vof) -op $(op) -ca giraph.masterComputeClass=$(masterCompute) -ca giraph.edgeValueClass=giraph.ml.grafos.okapi.spinner.EdgeValue -ca giraph.vertexValueClass=giraph.ml.grafos.okapi.spinner.VertexValue -ca giraph.outEdgesClass=giraph.ml.grafos.okapi.spinner.ShortBooleanHashMapsEdgesInOut -ca giraph.inputOutEdgesClass=giraph.ml.grafos.okapi.spinner.ShortBooleanHashMapsEdgesInOut -ca giraph.numComputeThreads=8,giraph.numInputThreads=8,giraph.numOutputThreads=1 -ca giraph.SplitMasterWorker=false -ca spinner.numberOfPartitions=$(partitions) -ca graph.directed=true -ca bgrap.SAMPLING_TYPE=$(sampling) -ca bgrap.GRAPH='$(graph)' -ca bgrap.DATE='$(date)' -ca rankdegree.TAU='$(tau)' -ca rankdegree.SIGMA='$(sigma)' -ca rankdegree.BETA='$(beta)' -ca rankdegree.SAVE_DD='$(save_dd)' 

run_optimized:
        giraph $(HADOOP_HOME)/lib/$(code) $(main) -w $(w) -vif $(vifo) -vip $(ip) -vof $(vofo) -op $(op) -ca giraph.masterComputeClass=$(masterCompute) -ca giraph.edgeValueClass=giraph.ml.grafos.okapi.spinner.EdgeValue -ca giraph.vertexValueClass=giraph.ml.grafos.okapi.spinner.VertexValue -ca giraph.outEdgesClass=giraph.lri.rrojas.rankdegree.ShortBooleanHashMapsEdgesInOut -ca giraph.inputOutEdgesClass=giraph.lri.rrojas.rankdegree.ShortBooleanHashMapsEdgesInOut -ca giraph.numComputeThreads=8,giraph.numInputThreads=8,giraph.numOutputThreads=1 -ca giraph.SplitMasterWorker=false -ca spinner.numberOfPartitions=$(partitions) -ca graph.directed=true -ca bgrap.SAMPLING_TYPE=$(sampling) -ca bgrap.GRAPH='$(graph)' -ca bgrap.DATE='$(date)' -ca rankdegree.TAU='$(tau)' -ca rankdegree.SIGMA='$(sigma)' -ca rankdegree.BETA='$(beta)' -ca rankdegree.SAVE_DD='$(save_dd)'

