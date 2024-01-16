import random
from pprint import pprint
from random import randint
from typing import Callable, Iterable

from pyspark import RDD, SparkContext

from dataclasses import dataclass

import fire


@dataclass
class Node:
    id: int
    color: int
    neighbors: list[int]
    candidate_color: int


def initialize_graph(n: int, max_degree: int) -> list[Node]:
    nodes = [Node(id=i, color=-1, neighbors=[], candidate_color=-1) for i in range(n)]
    for node in nodes:
        potential_neighbors = [i for i in range(n) if i != node.id]
        while len(node.neighbors) < randint(0, max_degree) and potential_neighbors:
            neighbor_id = random.choice(potential_neighbors)
            if len(nodes[neighbor_id].neighbors) < max_degree:
                node.neighbors.append(neighbor_id)
                nodes[neighbor_id].neighbors.append(node.id)  # For undirected graph
            potential_neighbors.remove(neighbor_id)
    return nodes


def choose_candidate_color(
    node: Node, neighbors: Iterable[int] | None, max_degree: int
) -> Node:
    if node.color != -1:
        return node

    available_colors = list(range(max_degree + 1))
    neighbor_colors = list(neighbors) if neighbors is not None else []
    available_colors = [c for c in available_colors if c not in neighbor_colors]
    if len(available_colors) > 0:
        candidate_color = random.choice(available_colors)
        return Node(
            id=node.id,
            color=-1,
            candidate_color=candidate_color,
            neighbors=node.neighbors,
        )

    return Node(id=node.id, color=-1, candidate_color=-1, neighbors=node.neighbors)


def apply_candidate_color(node: Node, neighbors: Iterable[int] | None) -> Node:
    if node.color != -1:
        return node

    neighbors_colors = list(neighbors) if neighbors is not None else []

    if len(neighbors_colors) == 0:
        print("NO_NEIGH", node)
        return Node(
            id=node.id,
            color=node.candidate_color,
            candidate_color=node.candidate_color,
            neighbors=node.neighbors,
        )

    if node.candidate_color != -1 and node.candidate_color not in neighbors_colors:
        print("CHOSEN", node, neighbors_colors)
        return Node(
            id=node.id,
            color=node.candidate_color,
            candidate_color=node.candidate_color,
            neighbors=node.neighbors,
        )

    return Node(id=node.id, color=-1, candidate_color=-1, neighbors=node.neighbors)


def get_joined_rdd(
    nodes_rdd: RDD[Node], accessor_fn: Callable[[Node], int]
) -> RDD[tuple[int, tuple[Node, Iterable[int] | None]]]:
    neighbors_rdd = (
        nodes_rdd.flatMap(
            lambda node: [
                (neighbor_id, accessor_fn(node)) for neighbor_id in node.neighbors
            ]
        )
        .mapValues(lambda node: [node])
        .reduceByKey(lambda a, b: a + b)
    )

    node_neighbors_rdd = nodes_rdd.map(lambda node: (node.id, node)).leftOuterJoin(
        neighbors_rdd, numPartitions=4
    )
    return node_neighbors_rdd


def is_coloring_valid(color_map: dict[int, int], nodes: list[Node]) -> bool:
    for node in nodes:
        node_color = color_map[node.id]
        for neighbor_id in node.neighbors:
            neighbor_color = color_map[neighbor_id]
            if node_color == neighbor_color:
                print(
                    f"Coloring condition failed: Node {node.id} and its neighbor {neighbor_id} have the same color."
                )
                return False
    return True


def color_graph_distributed(
    num_nodes: int = 1000, max_degree: int = 2, seed: int = 42
) -> None:
    random.seed(seed)
    sc = SparkContext.getOrCreate()
    nodes = initialize_graph(num_nodes, max_degree)

    nodes_rdd = sc.parallelize(nodes)
    num_left = nodes_rdd.count()

    i = 0

    while num_left > 0:
        print(f"Iter {i}: {num_left}")

        nodes_with_candidates = (
            get_joined_rdd(nodes_rdd, lambda x: x.color)
            .map(lambda kv: choose_candidate_color(kv[1][0], kv[1][1], max_degree))
            .cache()
        )

        new_nodes = get_joined_rdd(
            nodes_with_candidates, lambda x: x.candidate_color
        ).map(lambda kv: apply_candidate_color(kv[1][0], kv[1][1]))

        new_nodes.persist()
        nodes_rdd.unpersist()
        nodes_rdd = new_nodes
        num_left = new_nodes.filter(lambda node: node.color == -1).count()
        i += 1

    colored_graph = list(sorted(nodes_rdd.collect(), key=lambda node: node.id))

    pprint(colored_graph)

    colors = {node.id: node.color for node in colored_graph}

    pprint(colors)
    print(is_coloring_valid(colors, nodes))


if __name__ == "__main__":
    fire.Fire(color_graph_distributed)
