from taskproc import Task, Config


class CustomTask(Task):
    task_label = 'gpu'

    def run_task(self) -> None:
        print('Hi there')


__taskproc_config__ = Config(
        entrypoint=CustomTask,
        prefix={
            'gpu': 'jbsub -tty -queue x86_1h -cores 4+1 -mem 8g'
            }
        )
