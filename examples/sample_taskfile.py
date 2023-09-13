from taskproc import Task


class Main(Task):
    """ Example task """
    def run_task(self) -> None:
        print('Hi there')


if __name__ == '__main__':
    Main.cli()
