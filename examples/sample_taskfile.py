from pathlib import Path
from checkpoint import requires_directory, task


@task
def main():
    @requires_directory
    def __(path: Path) -> None:
        with open(path / 'test.txt', 'w') as f:
            f.write('hello\n')
        return
    return __
