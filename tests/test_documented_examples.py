"""Execute offline examples included in the documentation."""

import runpy
from pathlib import Path

import numpy.testing as npt


def test_climatology_config_example(capsys):
    example = Path(__file__).resolve().parents[1] / "docs/source/examples/climatology_config.py"
    namespace = runpy.run_path(str(example))

    assert len(namespace["collected"]) == 1
    result = namespace["collected"][0]
    assert result.package == "qartod"
    assert result.test == "climatology_test"
    npt.assert_array_equal(result.results, [1, 3, 9, 1, 3, 9] * 2)
    assert capsys.readouterr().out.strip() == "[1, 3, 9, 1, 3, 9, 1, 3, 9, 1, 3, 9]"
