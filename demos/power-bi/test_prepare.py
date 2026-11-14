import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("prepare_power_bi", HERE / "prepare.py")
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class PrepareTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((HERE / "config.example.json").read_text(encoding="utf-8"))
        self.config.update(parent_catalog="approved_catalog", parent_catalog_approved=True)
        self.config["sample_data"].update(date_rows=4, product_rows=3, customer_rows=2, sales_rows=8)
        self.star = prepare.StarSchemaConfig(date_rows=4, product_rows=3, customer_rows=2, sales_rows=8)

    def test_example_config_cannot_implicitly_use_catalog(self):
        with self.assertRaisesRegex(ValueError, "approved"):
            prepare.read_config(HERE / "config.example.json")

    def test_import_requires_explicit_approval(self):
        # read_config validates a file, but the committed template is never modified.
        self.config.update(storage_mode="Import", import_explicitly_approved=False)
        with patch.object(prepare.Path, "read_text", side_effect=lambda *args, **kwargs: json.dumps(self.config)):
            with self.assertRaisesRegex(ValueError, "Import requires"):
                prepare.read_config("unused")
            self.config["import_explicitly_approved"] = True
            config, _ = prepare.read_config("unused")
            self.assertEqual(config["storage_mode"], "Import")

    def test_generated_sql_has_constraints_checks_and_safe_batch(self):
        create, checks, change, expected = prepare.sql_artifacts(self.config, self.star)
        self.assertIn("CREATE SCHEMA `approved_catalog`.`fabricinterop_powerbi`", create)
        self.assertEqual(create.count("PRIMARY KEY"), 4)
        self.assertEqual(create.count("FOREIGN KEY"), 3)
        for columns in prepare.TABLE_COLUMN_TYPES.values():
            for name, sql_type in columns:
                self.assertIn(f"{prepare.ident(name)} {sql_type}", create)
        for source, target, key in prepare.FOREIGN_KEYS["FactSales"]:
            self.assertIn(
                f"FOREIGN KEY ({prepare.ident(source)}) REFERENCES "
                f"`approved_catalog`.`fabricinterop_powerbi`.{prepare.ident(target)} ({prepare.ident(key)})",
                create,
            )
        self.assertNotIn("OR REPLACE", create)
        self.assertNotIn("IF NOT EXISTS", create)
        self.assertEqual(checks.count("AS orphans"), 3)
        self.assertEqual(checks.count("SHOW CREATE TABLE"), 4)
        self.assertIn("BETWEEN 9 AND 18", change)
        self.assertEqual(expected["before"]["row_counts"]["FactSales"], 8)
        self.assertEqual(expected["after"]["row_counts"]["FactSales"], 18)
        self.assertGreater(float(expected["after"]["sales_total"]), float(expected["before"]["sales_total"]))

    def test_identifiers_escape_backticks(self):
        self.assertEqual(prepare.ident("catalog`name"), "`catalog``name`")
        with self.assertRaises(ValueError):
            prepare.ident("")


if __name__ == "__main__":
    unittest.main()
