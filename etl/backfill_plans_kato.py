"""
Backward-compatible wrapper for the incremental /v3/plans/kato worker.

Use:
  python etl/load_plans_kato_incremental.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.load_plans_kato_incremental import main


if __name__ == "__main__":
    asyncio.run(main())
