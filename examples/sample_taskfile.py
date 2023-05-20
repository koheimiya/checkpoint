from taskproc import infer_task_type, TaskBase


@infer_task_type
class Main(TaskBase):
    """ Example task """
    def run_task(self) -> None:
        raise NotImplementedError('Implement here')
