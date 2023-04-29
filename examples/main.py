from checkpoint import task


@task
def main():
    def __() -> None:
        print('hello')
        return
    return __
