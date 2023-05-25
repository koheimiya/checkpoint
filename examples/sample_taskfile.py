from taskproc import TaskBase


class Main(TaskBase):
    """ Example task """
    def run_task(self) -> None:
        print('Hi there')
        # raise NotImplementedError('Implement here')
