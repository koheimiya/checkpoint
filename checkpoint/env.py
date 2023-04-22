import os
from pathlib import Path


CHECKPOINT_PATH = Path(os.getenv('CHECKPOINT_DIR', './')) / '.checkpoints'
CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)


DISABLE_CHECKPOINT_REUSE = bool(os.getenv('DISABLE_CHECKPOINT', 0))
