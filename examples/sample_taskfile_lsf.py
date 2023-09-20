from taskproc import Task, CLIDefaultArguments


class Main(Task):
    task_label = 'gpu'

    def run_task(self) -> None:
        print('Hi there')


CLIDefaultArguments(
        prefix={
            'gpu': 'jbsub -tty -queue x86_1h -cores 4+1 -mem 8g'
            }
        ).populate()
