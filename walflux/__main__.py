"""``python -m walflux`` — the same entry point as the ``walflux`` script."""

from __future__ import annotations

import sys

from walflux.cli import main

if __name__ == "__main__":
    sys.exit(main())
