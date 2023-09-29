from taskproc import Task, DefaultCliArguments


class Main(Task):
    task_label = 'gpu'

    def run_task(self) -> None:
        print('Hi there')


DefaultCliArguments(
        prefix={
            'gpu': 'jbsub -tty -queue x86_1h -cores 4+1 -mem 8g'
            }
        ).populate()
