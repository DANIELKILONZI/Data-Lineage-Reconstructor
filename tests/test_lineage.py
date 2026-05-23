import unittest

from lineage import DataLineageReconstructor, Transformation


class DataLineageReconstructorTests(unittest.TestCase):
    def setUp(self):
        self.reconstructor = DataLineageReconstructor(
            [
                Transformation(
                    name="normalize_transactions",
                    inputs=("raw_transactions",),
                    outputs=("clean_transactions",),
                    pipeline="ingest",
                ),
                Transformation(
                    name="generate_risk_features",
                    inputs=("clean_transactions", "customer_profiles"),
                    outputs=("risk_features",),
                    pipeline="risk_scoring",
                ),
                Transformation(
                    name="build_audit_report",
                    inputs=("risk_features",),
                    outputs=("audit_report",),
                    pipeline="reporting",
                ),
            ]
        )

    def test_reconstruct_transformations_returns_all_transformations(self):
        transformations = self.reconstructor.reconstruct_transformations()

        self.assertEqual(3, len(transformations))
        self.assertEqual("normalize_transactions", transformations[0].name)

    def test_full_lineage_graph_contains_pipeline_transformation_dataset_edges(self):
        graph = self.reconstructor.full_lineage_graph()
        edge_ids = {(edge["from"], edge["to"], edge["type"]) for edge in graph["edges"]}

        self.assertIn(
            ("pipeline:risk_scoring", "transformation:generate_risk_features", "contains"),
            edge_ids,
        )
        self.assertIn(
            ("dataset:clean_transactions", "transformation:generate_risk_features", "consumed_by"),
            edge_ids,
        )
        self.assertIn(
            ("transformation:generate_risk_features", "dataset:risk_features", "produces"),
            edge_ids,
        )

    def test_trace_data_across_pipelines_finds_full_upstream_origin(self):
        graph = self.reconstructor.trace_data_across_pipelines("audit_report")
        node_ids = {node["id"] for node in graph["nodes"]}

        self.assertIn("dataset:audit_report", node_ids)
        self.assertIn("dataset:risk_features", node_ids)
        self.assertIn("dataset:clean_transactions", node_ids)
        self.assertIn("dataset:raw_transactions", node_ids)
        self.assertIn("pipeline:ingest", node_ids)
        self.assertIn("pipeline:risk_scoring", node_ids)
        self.assertIn("pipeline:reporting", node_ids)


if __name__ == "__main__":
    unittest.main()
