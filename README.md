# Distributed-Randomized-Coloring-Algorithm-

Requirements for running:  
pip install pyspark  
pip install fire   

The main script main.py supports the following command-line arguments:  
-n, --num_nodes: Number of nodes in the graph (default: 1000)  
-m, --max_degree: Maximum degree of the graph (default: 2)  
-s, --seed: Seed for randomization (default: 42)  
  
To run the algorithm, use the following command:  
python3 main.py -n <num_nodes> -m <max_degree> -s<seed>  
  
Running the script without parameters uses default values. To see available options and defaults, use:  
  python3 main.py --help   

Few examples inputs:  
 python3 main.py -n 100 -m 3 -s 40  
 python3 main.py -n 250 -m 2 -s 42  
 python3 main.py -n 300 -m 4 -s 45  
  
pprint(colored_graph) pretty prints the sorted list of nodes, displaying information about each node, such as its ID, color and its neighbors ID  
  
pprints(colors) pretty prints the colors dictionary, providing a compact view of the node IDs and their corresponding colors.  
  
TEST FUNCTION:  
pprint(is_coloring_valid(colors, nodes)) – calls test function is_coloring_valid which  checks whether the produced coloring is valid according to the graph's constraints.  
 Returns TRUE for valid coloring and FALSE for invalid coloring  

