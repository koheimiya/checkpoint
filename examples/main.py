from checkpoint import task


@task
def main():
    def __() -> str:
        return 'hello'
    return __
