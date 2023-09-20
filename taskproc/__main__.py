import sys
import os
from pathlib import Path

from taskproc import Config

from .task import Config, Task


print(sys.argv)
taskfile = Path(sys.argv[1])
module_name = taskfile.with_suffix('').name
sys.path.append(str(taskfile.parent))
pp = os.getenv('PYTHONPATH')
if pp is not None:
    os.environ['PYTHONPATH'] = ':'.join([str(taskfile.parent), pp])
else:
    os.environ['PYTHONPATH'] = str(taskfile.parent)
module = __import__(module_name)


# Run the main task
config = getattr(module, '__taskproc_config__', Config())
if config.entrypoint is None:
    entrypoint = getattr(module, 'Main')
else:
    entrypoint = config.entrypoint
assert issubclass(entrypoint, Task)

# defaults = config.default_args
# assert isinstance(defaults, DefaultArguments)

entrypoint.cli(args=sys.argv[2:], defaults=config)
