import unittest
from decimal import Decimal

from demos.common.star_schema import (
    TABLE_COLUMN_TYPES,
    StarSchemaConfig,
    generate_sales_batch,
    generate_star_schema,
    validate_star_schema,
)


class StarSchemaTests(unittest.TestCase):
    def setUp(self):
        self.config = StarSchemaConfig(
            date_rows=12,
            product_rows=4,
            customer_rows=8,
            sales_rows=24,
            seed=42,
        )

    def test_generation_is_deterministic_and_has_exact_decimal_totals(self):
        first = generate_star_schema(self.config)
        second = generate_star_schema(self.config)

        self.assertEqual(first, second)
        metrics = validate_star_schema(first, self.config)
        self.assertEqual(
            metrics["row_counts"],
            {
                "DimDate": 12,
                "DimProduct": 4,
                "DimCustomer": 8,
                "FactSales": 24,
            },
        )
        self.assertIsInstance(metrics["sales_total"], Decimal)
        self.assertEqual(
            sum(metrics["sales_by_category"].values(), Decimal("0.00")),
            metrics["sales_total"],
        )
        self.assertEqual(
            sum(metrics["sales_by_region"].values(), Decimal("0.00")),
            metrics["sales_total"],
        )
        self.assertEqual(
            sum(metrics["sales_by_month"].values(), Decimal("0.00")),
            metrics["sales_total"],
        )

    def test_validation_rejects_orphaned_fact_keys(self):
        tables = generate_star_schema(self.config)
        tables["FactSales"][0]["ProductKey"] = 999

        with self.assertRaisesRegex(ValueError, "has no DimProduct.ProductKey match"):
            validate_star_schema(tables, self.config)

    def test_batch_is_repeatable_and_uses_new_keys(self):
        first = generate_sales_batch(self.config, 25, 4)
        second = generate_sales_batch(self.config, 25, 4)

        self.assertEqual(first, second)
        self.assertEqual([row["SalesLineKey"] for row in first], [25, 26, 27, 28])
        self.assertTrue(all(isinstance(row["SalesAmount"], Decimal) for row in first))

    def test_config_must_exercise_every_dimension_key(self):
        invalid = StarSchemaConfig(
            date_rows=12,
            product_rows=4,
            customer_rows=8,
            sales_rows=7,
        )

        with self.assertRaisesRegex(ValueError, "cover every key"):
            generate_star_schema(invalid)

    def test_default_dataset_matches_documented_shape_and_total(self):
        metrics = validate_star_schema(generate_star_schema())

        self.assertEqual(
            metrics["row_counts"],
            {
                "DimDate": 365,
                "DimProduct": 20,
                "DimCustomer": 100,
                "FactSales": 10_000,
            },
        )
        self.assertEqual(metrics["sales_total"], Decimal("930299.36"))
        self.assertEqual(
            sum(metrics["sales_by_category"].values(), Decimal("0.00")),
            metrics["sales_total"],
        )
        self.assertEqual(
            sum(metrics["sales_by_region"].values(), Decimal("0.00")),
            metrics["sales_total"],
        )

    def test_generated_columns_match_shared_sql_contract(self):
        tables = generate_star_schema(self.config)

        for table_name, columns in TABLE_COLUMN_TYPES.items():
            self.assertEqual(set(tables[table_name][0]), {name for name, _ in columns})


if __name__ == "__main__":
    unittest.main()
