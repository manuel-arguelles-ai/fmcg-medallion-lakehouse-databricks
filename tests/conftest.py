import datetime as dt
import pytest

from datagen.generator import generate

SMALL = dict(seed=7, start=dt.date(2025, 1, 1), end=dt.date(2025, 2, 28), total_lines=8000,
             n_customers=(120, 60))


@pytest.fixture(scope="session")
def small_gen(tmp_path_factory):
    out = tmp_path_factory.mktemp("gen")
    manifest = generate(out, **SMALL)
    return out, manifest
