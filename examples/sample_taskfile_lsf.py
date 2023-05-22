from taskproc import infer_task_type, TaskBase


@infer_task_type
class Main(TaskBase, job_prefix=['jbsub -interactive -tty -queue x86_1h -cores 4+1 -mem 8g']):
    """ Example task """
    def run_task(self) -> None:
        print('Hi there')
        # raise NotImplementedError('Implement here')
