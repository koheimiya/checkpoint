import sys
import os
from pathlib import Path
from .task import Task


def main():
    # Load script
    taskfile = Path(sys.argv[1])
    module_name = taskfile.with_suffix('').name
    sys.path.append(str(taskfile.parent))
    pp = os.getenv('PYTHONPATH')
    if pp is not None:
        os.environ['PYTHONPATH'] = ':'.join([str(taskfile.parent), pp])
    else:
        os.environ['PYTHONPATH'] = str(taskfile.parent)
    module = __import__(module_name)


    # Run `Main` task
    entrypoint = getattr(module, 'Main')
    assert issubclass(entrypoint, Task)
    entrypoint.cli(args=sys.argv[2:])


if __name__ == '__main__':
    main()
