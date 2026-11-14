import copy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

from demos.common.preflight import (
    deployment_blockers,
    main,
    validate_local_config,
)


CONFIG_PATH = Path(__file__).parents[1] / "demos" / "config.example.json"


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

    def test_example_config_is_locally_valid_but_catalog_is_unapproved(self):
        self.assertEqual(validate_local_config(self.config), [])
        blockers = deployment_blockers(self.config, "all")
        self.assertIn(
            "Set an explicitly selected Databricks parent catalog.", blockers
        )
        self.assertIn(
            "Explicitly approve use of that parent catalog in configuration.",
            blockers,
        )

    def test_onelake_path_must_use_matching_ids(self):
        config = copy.deepcopy(self.config)
        config["fabric"]["workspaces"]["onelake"]["id"] = (
            "00000000-0000-0000-0000-000000000001"
        )
        config["onelake"]["storage_item_id"] = "00000000-0000-0000-0000-000000000002"
        config["onelake"]["abfss_url"] = (
            "abfss://00000000-0000-0000-0000-000000000001@"
            "onelake.dfs.fabric.microsoft.com/"
            "00000000-0000-0000-0000-000000000003/Files/"
        )

        self.assertIn(
            "OneLake ABFSS storage item ID must match the configured item",
            validate_local_config(config),
        )

    def test_secret_values_are_rejected(self):
        config = copy.deepcopy(self.config)
        config["auth"]["client_secret"] = "must-not-be-stored"

        self.assertTrue(
            any("secret references, not secret values" in error for error in validate_local_config(config))
        )

    def fully_confirmed_config(self):
        config = copy.deepcopy(self.config)
        config["environment"]["subscription_id"] = (
            "00000000-0000-0000-0000-000000000001"
        )
        config["deployment"]["approvals"].update(
            onelake_chargeable_resource_use=True,
            power_bi_chargeable_resource_use=True,
        )
        config["databricks"].update(
            workspace_url="https://example.azuredatabricks.net",
            workspace_id="00000000-0000-0000-0000-000000000002",
            workspace_confirmed=True,
            sql_warehouse_id="warehouse-id",
            parent_catalog="approved_catalog",
            parent_catalog_approved=True,
        )
        config["fabric"]["capacity_id"] = "00000000-0000-0000-0000-000000000003"
        config["fabric"]["workspaces"]["onelake"].update(
            id="00000000-0000-0000-0000-000000000004",
            confirmed=True,
        )
        config["fabric"]["workspaces"]["power_bi"].update(
            id="00000000-0000-0000-0000-000000000005",
            confirmed=True,
        )
        config["power_bi"]["workspace_id"] = config["fabric"]["workspaces"][
            "power_bi"
        ]["id"]
        config["onelake"].update(
            storage_item_id="00000000-0000-0000-0000-000000000006",
            fabric_mirror_id="00000000-0000-0000-0000-000000000007",
            storage_credential_name="approved_credential",
            external_location_name="approved_location",
            identity_reference="approved-access-identity",
            tenant_short_lived_sas_key_generation="enabled",
            workspace_user_delegated_sas_authentication="enabled",
            file_events_mode="disabled",
        )
        config["onelake"]["abfss_url"] = (
            "abfss://00000000-0000-0000-0000-000000000004@"
            "onelake.dfs.fabric.microsoft.com/"
            "00000000-0000-0000-0000-000000000006/Files/"
        )
        return config

    def test_require_deployment_needs_workspace_charge_and_capacity_confirmation(self):
        config = self.fully_confirmed_config()
        self.assertEqual(validate_local_config(config), [])
        self.assertEqual(deployment_blockers(config, "onelake"), [])
        self.assertEqual(deployment_blockers(config, "power-bi"), [])

        config["databricks"]["workspace_confirmed"] = False
        config["deployment"]["approvals"]["power_bi_chargeable_resource_use"] = False
        config["fabric"]["capacity_id"] = ""
        blockers = deployment_blockers(config, "power-bi")
        self.assertIn("Confirm the selected Databricks workspace in configuration.", blockers)
        self.assertIn(
            "Obtain explicit approval for Power BI demo chargeable-resource use.",
            blockers,
        )
        self.assertIn("Resolve the Fabric capacity ID for the Power BI demo.", blockers)

        config["databricks"]["workspace_confirmed"] = True
        config["deployment"]["approvals"]["onelake_chargeable_resource_use"] = False
        config["fabric"]["workspaces"]["onelake"]["confirmed"] = False
        onelake_blockers = deployment_blockers(config, "onelake")
        self.assertIn(
            "Confirm the selected OneLake Fabric workspace in configuration.",
            onelake_blockers,
        )
        self.assertIn(
            "Obtain explicit approval for OneLake demo chargeable-resource use.",
            onelake_blockers,
        )

    def test_power_bi_workspace_ids_must_match(self):
        config = self.fully_confirmed_config()
        config["power_bi"]["workspace_id"] = "00000000-0000-0000-0000-000000000008"
        self.assertIn(
            "The Fabric and Power BI workspace IDs must match for this demo.",
            deployment_blockers(config, "power-bi"),
        )

    def test_sql_warehouse_is_required_only_for_power_bi_deployment(self):
        config = self.fully_confirmed_config()
        config["databricks"]["sql_warehouse_id"] = ""

        self.assertEqual(deployment_blockers(config, "onelake"), [])
        self.assertIn(
            "Resolve the existing Databricks SQL warehouse ID.",
            deployment_blockers(config, "power-bi"),
        )

    def test_onelake_file_event_role_and_resource_approvals_are_separate(self):
        config = self.fully_confirmed_config()
        config["onelake"]["file_events_mode"] = "automatic"
        blockers = deployment_blockers(config, "onelake")
        self.assertIn(
            "Obtain separate approval for any required Fabric file-event identity role.",
            blockers,
        )
        self.assertIn(
            "Obtain separate approval for any required Fabric file eventstream provisioning.",
            blockers,
        )

        config["onelake"]["file_event_role_escalation_approved"] = True
        blockers = deployment_blockers(config, "onelake")
        self.assertNotIn(
            "Obtain separate approval for any required Fabric file-event identity role.",
            blockers,
        )
        self.assertIn(
            "Obtain separate approval for any required Fabric file eventstream provisioning.",
            blockers,
        )
        config["onelake"]["file_eventstream_provisioning_approved"] = True
        self.assertEqual(deployment_blockers(config, "onelake"), [])

    def test_onelake_file_events_must_be_verified_before_deployment(self):
        config = self.fully_confirmed_config()
        config["onelake"]["file_events_mode"] = "unverified"
        self.assertIn(
            "Verify and record the OneLake external-location file-events mode.",
            deployment_blockers(config, "onelake"),
        )

    def test_require_deployment_returns_blocked_exit_code(self):
        config = self.fully_confirmed_config()
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            output = io.StringIO()
            with redirect_stdout(output):
                configured_result = main(
                    [
                        "--config",
                        str(config_path),
                        "--demo",
                        "power-bi",
                        "--require-deployment",
                    ]
                )
            self.assertEqual(configured_result, 0)
            self.assertIn("deployment readiness: CONFIGURED", output.getvalue())

            config["fabric"]["workspaces"]["power_bi"]["confirmed"] = False
            config_path.write_text(json.dumps(config), encoding="utf-8")
            output = io.StringIO()
            with redirect_stdout(output):
                blocked_result = main(
                    [
                        "--config",
                        str(config_path),
                        "--demo",
                        "power-bi",
                        "--require-deployment",
                    ]
                )
        self.assertEqual(blocked_result, 2)
        self.assertIn("deployment readiness: BLOCKED", output.getvalue())


if __name__ == "__main__":
    unittest.main()
