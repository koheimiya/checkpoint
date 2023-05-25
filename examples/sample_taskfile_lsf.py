from taskproc import TaskBase


class Main(TaskBase, job_prefix=['jbsub -interactive -tty -queue x86_1h -cores 4+1 -mem 8g']):
    """ Example task """
    def run_task(self) -> None:
        print('Hi there')
        # raise NotImplementedError('Implement here')
