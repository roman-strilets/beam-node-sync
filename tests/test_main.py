from pathlib import Path

import pytest

import main as cli_main
from src.models import DeriveResult, StageResult


def test_main_rejects_non_positive_start_height(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        cli_main.main(["node.example:8100", "--start-height", "0"])

    captured = capsys.readouterr()
    assert "start-height must be > 0" in captured.err


def test_main_rejects_start_height_above_stop_height(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit):
        cli_main.main(["node.example:8100", "--start-height", "11", "--stop-height", "10"])

    captured = capsys.readouterr()
    assert "start-height must be <= stop-height" in captured.err


def test_main_requires_node(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        cli_main.main([])

    captured = capsys.readouterr()
    assert "the following arguments are required: node" in captured.err


def test_main_passes_staged_config_to_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_config = None

    def fake_run_staged(config):
        nonlocal captured_config
        captured_config = config
        return (
            StageResult(
                node="node.example:8100",
                target_height=9,
                staged_height=9,
                staged_blocks=9,
                duration_seconds=0.0,
            ),
            DeriveResult(
                target_height=9,
                synced_height=9,
                applied_blocks=0,
                outputs_seen=0,
                resolved_spends=0,
                unresolved_spends=0,
                duration_seconds=0.0,
            ),
        )

    monkeypatch.setattr(cli_main, "run_staged", fake_run_staged)

    exit_code = cli_main.main(
        [
            "node.example:8100",
            "--state-db",
            str(tmp_path / "state.sqlite3"),
            "--start-height",
            "7",
            "--stop-height",
            "9",
            "--fast-sync",
        ]
    )

    assert exit_code == 0
    assert captured_config is not None
    assert captured_config.start_height == 7
    assert captured_config.stop_height == 9
    assert captured_config.fast_sync is True


def test_main_rejects_removed_mode_argument(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        cli_main.main(["node.example:8100", "--mode", "staged"])

    captured = capsys.readouterr()
    assert "unrecognized arguments: --mode staged" in captured.err


def test_main_rejects_removed_output_argument(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        cli_main.main(["node.example:8100", "--output", "utxos.jsonl"])

    captured = capsys.readouterr()
    assert "unrecognized arguments: --output" in captured.err