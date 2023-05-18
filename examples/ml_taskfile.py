from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, NewType, Protocol, TypeVar

from cloudpickle import dump, load
from checkpoint import infer_task_type, Task, TaskLike, Requires, RequiresList


# For demonstration
Data = NewType('Data', str)
Model = NewType('Model', str)


T_co = TypeVar('T_co', covariant=True)
class Loader(Protocol[T_co]):
    def load(self) -> T_co:
        ...


@dataclass
class PickleLoader(Generic[T_co]):
    def __init__(self, obj: T_co, path: Path) -> None:
        self.path = path
        dump(obj, open(self.path, 'wb'))

    def load(self) -> T_co:
        return load(open(self.path, 'rb'))


@infer_task_type
class LoadData(Task):
    def build_task(self, name: str):
        self.name = name

    def run_task(self) -> Loader[Data]:
        # Download a dataset ...
        data_loader = PickleLoader(Data(f'<data: {self.name}>'), self.task_directory / 'data.txt')
        return data_loader


@infer_task_type
class PreprocessData(Task):
    raw_data: Requires[Loader[Data]]

    def build_task(self, name: str, split_ratio: float, seed: int):
        self.name = name
        self.split_ratio = split_ratio
        self.seed = seed
        self.raw_data = LoadData(name)

    def run_task(self) -> dict[str, Loader[Data]]:
        data = self.raw_data.load()
        # Split the dataset at data_path into splits ...
        train_loader = PickleLoader(Data('<train data>'), self.task_directory / 'train.txt')
        valid_loader = PickleLoader(Data('<valid data>'), self.task_directory / 'valid.txt')
        test_loader = PickleLoader(Data('<test data>'), self.task_directory / 'test.txt')
        return {'train': train_loader, 'valid': valid_loader, 'test': test_loader}


@infer_task_type
class TrainModel(Task):
    train_data: Requires[Loader[Data]]
    valid_data: Requires[Loader[Data]]
    
    def build_task(self, train: TaskLike[Loader[Data]], valid: TaskLike[Loader[Data]], train_config: dict, seed: int):
        self.train_data = train
        self.valid_data = valid
        self.train_config = train_config
        self.seed = seed
    
    def run_task(self) -> Loader[Model]:
        train_data = self.train_data.load()
        valid_data = self.valid_data.load()
        # Train model with data and save it to trained_path ...
        return PickleLoader(Model('<trained model>'), self.task_directory / 'trained.bin')


@infer_task_type
class TestModel(Task):
    test_data: Requires[Loader[Data]]
    model: Requires[Loader[Model]]

    def build_task(self, test: TaskLike[Loader[Data]], trained_model: Task[Loader[Model]]):
        self.test_data = test
        self.model = trained_model
    
    def run_task(self) -> dict[str, Any]:
        test_data = self.test_data.load()
        model = self.model.load()
        # Evaluate model on test_data ...
        result = {'score': ...}
        return result


@infer_task_type
class Main(Task):
    results: RequiresList[dict]

    def build_task(self):
        tasks: list[Task[dict]] = []
        for i in range(10):
            dataset = PreprocessData('mydata', split_ratio=.8, seed=i)
            trained = TrainModel(
                    train=dataset['train'],
                    valid=dataset['valid'],
                    train_config={'lr': .01},
                    seed=i
                    )
            result = TestModel(
                    test=dataset['test'],
                    trained_model=trained
                    )
            tasks.append(result)
        self.results = tasks

    def run_task(self) -> list[dict]:
        print('running main')
        return self.results
