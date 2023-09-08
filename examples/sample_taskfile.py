from taskproc import TaskBase


class Main(TaskBase):
    """ Example task """
    def run_task(self) -> None:
        print('Hi there')


if __name__ == '__main__':
    Main.cli()
