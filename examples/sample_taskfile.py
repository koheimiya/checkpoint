from taskprocessing import infer_task_type, Task


@infer_task_type
class Main(Task):
    """ Example task """

    def build_task(self):
        pass

    def run_task(self) -> None:
        print('running')
        with open(self.task_directory / 'text.txt', 'w') as f:
            f.write('hello\n')
        return
