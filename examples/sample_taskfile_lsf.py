from taskproc import TaskBase


class Main(TaskBase, prefix_command='jbsub -interactive -tty -queue x86_1h -cores 4+1 -mem 8g'):
    """ Example task """
    def run_task(self) -> None:
        print('Hi there')
