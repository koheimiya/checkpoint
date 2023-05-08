from checkpoint import infer_task_type, Task, DataPath


@infer_task_type
class Main(Task):
    """ Example task """
    file = DataPath('text.txt')

    def init(self):
        pass

    def main(self) -> None:
        print('running')
        with open(self.file, 'w') as f:
            f.write('hello\n')
        return
