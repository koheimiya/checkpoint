from typing import NewType, reveal_type
from taskproc import Task, Future
from taskproc.future import FutureList


Data = NewType('Data', str)
Model = NewType('Model', str)


class LoadData(Task):
    def __init__(self, name: str):
        self.name = name

    def run_task(self):
        return Data('raw data')


class PreprocessData(Task):
    def __init__(self, name: str, split_ratio: float, seed: int):
        self.name = name
        self.split_ratio = split_ratio
        self.seed = seed
        self.raw_data = LoadData(name)

    def run_task(self):
        train = Data(f'train data ({self.name}, {self.split_ratio}, {self.seed})')
        valid = Data(f'valid data ({self.name}, {self.split_ratio}, {self.seed})')
        test  = Data(f'test data ({self.name}, {self.split_ratio}, {self.seed})')
        return {'train': train, 'valid': valid, 'test': test}


class TrainModel(Task):
    def __init__(self, train: Future[Data], valid: Future[Data], train_config: dict, seed: int):
        self.train_data = train
        self.valid_data = valid
        self.train_config = train_config
        self.seed = seed
    
    def run_task(self):
        model = Model(f"trained model ({self.train_config}, {self.seed})")
        return model


class TestModel(Task):
    def __init__(self, test: Future[Data], trained_model: Future[Model]):
        self.test_data = test
        self.model = trained_model
    
    def run_task(self):
        return {'score': 42}


class Main(Task):
    def __init__(self):
        self.results = FutureList[dict[str, int]]()
        for i in range(10):
            dataset = PreprocessData('mydata', split_ratio=.8, seed=i)
            trained = TrainModel(
                    train=dataset['train'],
                    valid=dataset['valid'],
                    train_config={'lr': .01},
                    seed=i,
                    )
            result = TestModel(
                    test=dataset['test'],
                    trained_model=trained,
                    )
            self.results.append(result)

    def run_task(self) -> None:
        print('Running main')
        scores = [res['score'] for res in self.results.get_result()]
        print(scores)


if __name__ == '__main__':
    Main.cli()
