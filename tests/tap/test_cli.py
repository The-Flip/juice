"""Tests for the tap CLI's guards -- the parts that refuse, not the parts that run."""

from __future__ import annotations

from click.testing import CliRunner

from tap.cli import cli


class TestBench:
    def test_bench_refuses_a_live_buffer_dir(self, tmp_path):
        """`tap bench --buffer-dir <live dir>` used to write `BENCH0000...`
        outlets into the one devices table the roster is read from, and from
        then on every roster frame carried a dozen fake strips."""
        (tmp_path / "meta.sqlite").write_bytes(b"")
        result = CliRunner().invoke(cli, ["bench", "--buffer-dir", str(tmp_path), "--ticks", "1"])
        assert result.exit_code == 2, result.output
        assert "live buffer" in result.output
        assert not list(tmp_path.glob("readings-*.sqlite"))

    def test_bench_runs_in_an_empty_dir(self, tmp_path):
        result = CliRunner().invoke(
            cli, ["bench", "--buffer-dir", str(tmp_path / "b"), "--ticks", "2", "--devices", "1"]
        )
        assert result.exit_code == 0, result.output
        assert "rows written" in result.output
