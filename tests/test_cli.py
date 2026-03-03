"""Tests for the CLI interface."""

import json

from click.testing import CliRunner

from crdb_analyzer.cli import main


class TestCLI:
    def test_version(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        for cmd in [
            "hot-ranges", "hot-nodes", "data-skew", "table-stats",
            "contention", "range-details", "index-usage", "lease-balance",
            "stmt-fingerprints", "cluster-health", "snapshot", "history",
            "compare", "daemon",
        ]:
            assert cmd in result.output, f"{cmd} missing from --help"

    def test_hot_ranges_from_file(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))

        runner = CliRunner()
        result = runner.invoke(main, ["hot-ranges", "--from-file", str(f)])
        assert result.exit_code == 0
        assert "Hot Ranges" in result.output

    def test_hot_ranges_json_format(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))

        runner = CliRunner()
        result = runner.invoke(main, ["--format", "json", "hot-ranges", "--from-file", str(f)])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "rows" in parsed

    def test_hot_nodes_from_file(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))

        runner = CliRunner()
        result = runner.invoke(main, ["hot-nodes", "--from-file", str(f)])
        assert result.exit_code == 0
        assert "Node Load" in result.output

    def test_data_skew_from_file(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))

        runner = CliRunner()
        result = runner.invoke(main, ["data-skew", "--from-file", str(f)])
        assert result.exit_code == 0
        assert "Data Skew" in result.output

    def test_no_connection_error(self):
        runner = CliRunner()
        result = runner.invoke(main, ["hot-ranges"])
        assert result.exit_code == 1
        assert "Error" in result.output

    def test_csv_output(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))

        runner = CliRunner()
        result = runner.invoke(main, ["--format", "csv", "hot-ranges", "--from-file", str(f)])
        assert result.exit_code == 0
        assert "range_id" in result.output

    def test_daemon_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "--help"])
        assert result.exit_code == 0
        assert "--interval" in result.output
        assert "--analyses" in result.output
        assert "--retention-days" in result.output

    def test_daemon_invalid_analysis(self):
        runner = CliRunner()
        result = runner.invoke(main, [
            "--sql-url", "postgresql://root@localhost:26257/defaultdb",
            "daemon", "--analyses", "hot-ranges,bogus",
        ])
        assert result.exit_code == 1
        assert "unknown analyses" in result.output

    def test_daemon_no_connection(self):
        runner = CliRunner()
        result = runner.invoke(main, ["daemon"])
        assert result.exit_code == 1
        assert "requires" in result.output

    def test_save_flag_creates_snapshot(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))
        db = tmp_path / "snap.db"

        runner = CliRunner()
        result = runner.invoke(main, [
            "--snapshot-db", str(db),
            "hot-ranges", "--from-file", str(f), "--save",
        ])
        assert result.exit_code == 0
        assert "Snapshot saved" in result.output

    def test_history_list(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))
        db = tmp_path / "snap.db"

        runner = CliRunner()
        runner.invoke(main, [
            "--snapshot-db", str(db),
            "hot-ranges", "--from-file", str(f), "--save",
        ])
        result = runner.invoke(main, [
            "--snapshot-db", str(db),
            "history",
        ])
        assert result.exit_code == 0
        assert "hot-ranges" in result.output

    def test_history_show(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))
        db = tmp_path / "snap.db"

        runner = CliRunner()
        save_result = runner.invoke(main, [
            "--snapshot-db", str(db),
            "hot-ranges", "--from-file", str(f), "--save",
        ])
        sid = save_result.output.split("Snapshot saved: ")[1].split("\n")[0].strip()

        result = runner.invoke(main, [
            "--snapshot-db", str(db),
            "history", "--show", sid,
        ])
        assert result.exit_code == 0
        assert "Snapshot" in result.output

    def test_compare(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))
        db = tmp_path / "snap.db"

        runner = CliRunner()
        r1 = runner.invoke(main, [
            "--snapshot-db", str(db),
            "hot-ranges", "--from-file", str(f), "--save",
        ])
        sid1 = r1.output.split("Snapshot saved: ")[1].split("\n")[0].strip()

        r2 = runner.invoke(main, [
            "--snapshot-db", str(db),
            "hot-ranges", "--from-file", str(f), "--save",
        ])
        sid2 = r2.output.split("Snapshot saved: ")[1].split("\n")[0].strip()

        result = runner.invoke(main, [
            "--snapshot-db", str(db),
            "compare", sid1, sid2,
        ])
        assert result.exit_code == 0
        assert "Compare" in result.output
