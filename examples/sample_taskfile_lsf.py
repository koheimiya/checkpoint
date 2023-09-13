from taskproc import Task


class Main(Task):
    _task_prefix_command: str = 'jbsub -interactive -tty -queue x86_1h -cores 4+1 -mem 8g'
    """ Example task """
    def run_task(self) -> None:
        print('Hi there')


if __name__ == '__main__':
    Main.cli()
