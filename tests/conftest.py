from collections.abc import Iterator

import pytest
from src import search_store


@pytest.fixture(autouse=True)
def _mapping_check_already_done() -> Iterator[None]:
    """Keep the once-per-process mapping upgrade out of tests that script exact requests.

    Tests of the upgrade itself clear the set explicitly.
    """
    search_store._MAPPINGS_CHECKED.clear()
    search_store._MAPPINGS_CHECKED.update({"hl7-messages-v1", "ccda-documents-v1"})
    yield
    search_store._MAPPINGS_CHECKED.clear()
