from pathlib import Path
from checkpoint import requires_directory, task


@task
def main():
    """ Example task """
    @requires_directory
    def __(path: Path) -> None:
        print('running')
        with open(path / 'test.txt', 'w') as f:
            f.write('hello\n')
        return
    return __
