from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Transformation:
    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    pipeline: str


class DataLineageReconstructor:
    """Reconstruct data lineage across multi-pipeline transformations."""

    def __init__(self, transformations: Iterable[Transformation]):
        self._transformations = list(transformations)
        self._producers_by_output: dict[str, list[Transformation]] = defaultdict(list)

        for transformation in self._transformations:
            for output in transformation.outputs:
                self._producers_by_output[output].append(transformation)

    def reconstruct_transformations(self) -> list[Transformation]:
        return list(self._transformations)

    def full_lineage_graph(self) -> dict[str, list[dict[str, str]]]:
        nodes: dict[str, dict[str, str]] = {}
        edges: set[tuple[str, str, str]] = set()

        for transformation in self._transformations:
            transformation_id = f"transformation:{transformation.name}"
            pipeline_id = f"pipeline:{transformation.pipeline}"
            nodes[transformation_id] = {
                "id": transformation_id,
                "type": "transformation",
                "name": transformation.name,
                "pipeline": transformation.pipeline,
            }
            nodes[pipeline_id] = {
                "id": pipeline_id,
                "type": "pipeline",
                "name": transformation.pipeline,
            }
            edges.add((pipeline_id, transformation_id, "contains"))

            for dataset in transformation.inputs:
                dataset_id = f"dataset:{dataset}"
                nodes[dataset_id] = {"id": dataset_id, "type": "dataset", "name": dataset}
                edges.add((dataset_id, transformation_id, "consumed_by"))

            for dataset in transformation.outputs:
                dataset_id = f"dataset:{dataset}"
                nodes[dataset_id] = {"id": dataset_id, "type": "dataset", "name": dataset}
                edges.add((transformation_id, dataset_id, "produces"))

        return {
            "nodes": sorted(nodes.values(), key=lambda item: item["id"]),
            "edges": [
                {"from": source, "to": target, "type": edge_type}
                for source, target, edge_type in sorted(edges)
            ],
        }

    def trace_data_across_pipelines(self, target_dataset: str) -> dict[str, list[dict[str, str]]]:
        full_graph = self.full_lineage_graph()
        nodes_by_id = {node["id"]: node for node in full_graph["nodes"]}

        relevant_node_ids: set[str] = set()
        relevant_edge_ids: set[tuple[str, str, str]] = set()

        queue = deque([target_dataset])
        seen_datasets: set[str] = set()

        while queue:
            dataset = queue.popleft()
            if dataset in seen_datasets:
                continue
            seen_datasets.add(dataset)

            dataset_id = f"dataset:{dataset}"
            if dataset_id in nodes_by_id:
                relevant_node_ids.add(dataset_id)

            for producer in self._producers_by_output.get(dataset, []):
                transformation_id = f"transformation:{producer.name}"
                pipeline_id = f"pipeline:{producer.pipeline}"
                relevant_node_ids.update({transformation_id, pipeline_id})
                relevant_edge_ids.add((pipeline_id, transformation_id, "contains"))
                relevant_edge_ids.add((transformation_id, dataset_id, "produces"))

                for producer_input in producer.inputs:
                    input_id = f"dataset:{producer_input}"
                    relevant_node_ids.add(input_id)
                    relevant_edge_ids.add((input_id, transformation_id, "consumed_by"))
                    queue.append(producer_input)

        return {
            "nodes": [nodes_by_id[node_id] for node_id in sorted(relevant_node_ids)],
            "edges": [
                {"from": source, "to": target, "type": edge_type}
                for source, target, edge_type in sorted(relevant_edge_ids)
            ],
        }
