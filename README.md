# Distributed-Randomized-Coloring-Algorithm-

Requirements for running:
pip install pyspark
pip install fire 


The main script main.py supports the following command-line arguments:

  -n, --num_nodes: Number of nodes in the graph (default: 10000)
  -m, --max_degree: Maximum degree of the graph (default: 2)
  -s, --seed: Seed for randomization (default: 42)

To run the algorithm, use the following command:
  python3 main.py -n <num_nodes> -m <max_degree> -s <seed>


Running the script without parameters uses default values. To see available options and defaults, use:
  python3 main.py --help



