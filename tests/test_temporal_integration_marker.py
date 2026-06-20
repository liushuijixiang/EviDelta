from __future__ import annotations

import os
import subprocess

import pytest


@pytest.mark.temporal_integration
def test_monitoring_schedule_script_against_local_temporal():
    if os.environ.get("RUN_TEMPORAL_INTEGRATION") != "1":
        pytest.skip("set RUN_TEMPORAL_INTEGRATION=1 to use local Temporal")

    result = subprocess.run(
        ["./scripts/test_monitoring_schedule.sh"],
        cwd=".",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
