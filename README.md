# BGRAP

The definition of effective strategies for graph partitioning is a major challenge in distributed environments since an effective graph partitioning allows to considerably improve the performance of large graph data analytics computations. In this paper, we propose a multi-objective and scalable Balanced GRAph Partitioning (B-GRAP) algorithm to produce balanced graph partitions. B-GRAP is based on Label Propagation (LP) approach and defines different objective functions to deal with either vertex or edge balance constraints while considering edge direction in graphs. The experiments are performed on various graphs while varying the number of partitions. We evaluate B-GRAP using several quality measures and the computation time. The results show that B-GRAP (i) provides a good balance while reducing the cuts between the different computed partitions (ii) reduces the global computation time, compared to Spinner algorithm.

Cite this repository as:
Moussawi A.E., Seghouani N.B., Bugiotti F. (2020) A Graph Partitioning Algorithm for Edge or Vertex Balance. In: Hartmann S., Küng J., Kotsis G., Tjoa A.M., Khalil I. (eds) Database and Expert Systems Applications. DEXA 2020. Lecture Notes in Computer Science, vol 12391. Springer, Cham. https://doi.org/10.1007/978-3-030-59003-1_2


The "giraph" package contains the main paratitioning algorithms, analytic algorithms, formatters, etc.
The "Example" package contains a Java class to configure a Giraph Job with a Hadoop cluster setting and a class to run an example of BGRAP algorithm.


"giraph/lri/aelmoussawi/partitioning/" contains the orignal code BGRAP

"giraph/lri/rrojas/rankdegree/" contains an optimized of BGRAP (memory usage reduced) and new versions that use graph sampling to initialize the algorithm
