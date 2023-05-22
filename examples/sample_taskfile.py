from taskproc import infer_task_type, TaskBase


@infer_task_type
class Main(TaskBase):
    """ Example task """
    def run_task(self) -> None:
        print('Hi there')
        # raise NotImplementedError('Implement here')
