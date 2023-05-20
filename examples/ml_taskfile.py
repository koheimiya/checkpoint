from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, NewType, Protocol, TypeVar

from cloudpickle import dump, load
from taskproc import infer_task_type, TaskBase, Task, Requires, RequiresList


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
class LoadData(TaskBase):
    def build_task(self, name: str):
        self.name = name

    def run_task(self) -> Loader[Data]:
        # Download a dataset ...
        data_loader = PickleLoader(Data(f'<data: {self.name}>'), self.task_directory / 'data.txt')
        return data_loader


@infer_task_type
class PreprocessData(TaskBase):
    raw_data_loader: Requires[Loader[Data]]

    def build_task(self, name: str, split_ratio: float, seed: int):
        self.name = name
        self.split_ratio = split_ratio
        self.seed = seed
        self.raw_data_loader = LoadData(name)

    def run_task(self) -> dict[str, Loader[Data]]:
        data = self.raw_data_loader.load()
        # Split the dataset at data_path into splits ...
        train_loader = PickleLoader(Data('<train data>'), self.task_directory / 'train.txt')
        valid_loader = PickleLoader(Data('<valid data>'), self.task_directory / 'valid.txt')
        test_loader = PickleLoader(Data('<test data>'), self.task_directory / 'test.txt')
        return {'train': train_loader, 'valid': valid_loader, 'test': test_loader}


@infer_task_type
class TrainModel(TaskBase):
    train_loader: Requires[Loader[Data]]
    valid_loader: Requires[Loader[Data]]
    
    def build_task(self, train: Task[Loader[Data]], valid: Task[Loader[Data]], train_config: dict, seed: int):
        self.train_loader = train
        self.valid_loader = valid
        self.train_config = train_config
        self.seed = seed
    
    def run_task(self) -> Loader[Model]:
        train_data = self.train_loader.load()
        valid_data = self.valid_loader.load()
        # Train model with data and save it to trained_path ...
        return PickleLoader(Model('<trained model>'), self.task_directory / 'trained.bin')


@infer_task_type
class TestModel(TaskBase):
    test_loader: Requires[Loader[Data]]
    model: Requires[Loader[Model]]

    def build_task(self, test: Task[Loader[Data]], trained_model: Task[Loader[Model]]):
        self.test_loader = test
        self.model = trained_model
    
    def run_task(self) -> dict[str, Any]:
        test_data = self.test_loader.load()
        model = self.model.load()
        # Evaluate model on test_data ...
        result = {'score': ...}
        return result


@infer_task_type
class Main(TaskBase):
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

    def run_task(self) -> None:
        print('run main')
        return
