# WABC beta 1.0

### Requirements ###
I.E. https://hama.apache.org/getting_started_with_hama.html

hadoop-1.0.x or higher version
Sun Java JDK 1.7.x or higher version
SSH access to manage BSP deamons
Apache Hama

1) Install all the component required for the environment 
2) Load the project in the preferred IDE, together with the Hama classes under the package org\apache\hama\graph\source
3) Build the project and generate the snapshopt
4) Move your input json to HDFS
5) Run the snapshot.jar as: $HAMA_HOME/bin/hama jar $snapshotName.jar -i $pathInput -o $pathOutput -t $number_of_task -f JSON (input file must be a JSON)

Note that the input file must be a JSON like: 
[vertex_id, 1 if is seed else 0, ranking value of vertex, [[vertex_id_dest, probability, ranking value of vertex_id_det], [vertex_id_dest2, probability, ranking_value_of_vertex_id_dest].....]

Note that it's possible customize the algorithm changing some parameters such as
- DISTANCE: determine the maximum influence path length, default -1 
- NUMBER_OF_TOP: determine the number of neighbors that the algorithm will evaluate as new seed (Employer bee), default Integer.MAX_INT
- STOPPING_PERC_VALUE: or also omega, that represent the convergence threshold default 0.02
- WEIGHTEDALGORITHM: that change the fitness evaluation. If true the function will consider the probability of influence, else no default true
- THRESHOLD AND LIMIT: two params that can include a non deterministic source for the algoritm. When a node try to influence another node, it's performs $limit attempt and activate the target node if successful attempts are at least $threshold. default -1 and 1
