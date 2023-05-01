from pathlib import Path
from checkpoint import requires, TaskDirectory, taskflow


@taskflow
def main():
    """ Example task """
    @requires(TaskDirectory())
    def __(path: Path) -> None:
        print('running')
        with open(path / 'test.txt', 'w') as f:
            f.write('hello\n')
        return
    return __
