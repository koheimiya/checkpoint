from checkpoint import infer_task_type, Task, DataPath


@infer_task_type
class Main(Task):
    """ Example task """
    file = DataPath('text.txt')

    def build_task(self):
        pass

    def run_task(self) -> None:
        print('running')
        with open(self.file, 'w') as f:
            f.write('hello\n')
        return
