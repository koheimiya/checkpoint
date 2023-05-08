from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, NewType, Protocol, TypeVar

from cloudpickle import dump, load
from checkpoint import infer_task_type, Task, DataPath, Requires, RequiresList


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
    data = DataPath('data.txt')

    def init(self, name: str):
        self.name = name

    def main(self) -> Loader[Data]:
        # Download a dataset ...
        data_loader = PickleLoader(Data(f'<data: {self.name}>'), self.data)
        return data_loader


@infer_task_type
class PreprocessData(Task):
    data_loader: Requires[Loader[Data]]
    train_path = DataPath('train.txt')
    valid_path = DataPath('valid.txt')
    test_path = DataPath('test.txt')

    def init(self, name: str, split_ratio: float, seed: int):
        self.name = name
        self.split_ratio = split_ratio
        self.seed = seed
        self.data_loader = LoadData(name)

    def main(self) -> dict[str, Loader[Data]]:
        data = self.data_loader.load()
        # Split the dataset at data_path into splits ...
        train_loader = PickleLoader(Data('<train data>'), self.train_path)
        valid_loader = PickleLoader(Data('<valid data>'), self.valid_path)
        test_loader = PickleLoader(Data('<test data>'), self.test_path)
        return {'train': train_loader, 'valid': valid_loader, 'test': test_loader}


@infer_task_type
class TrainModel(Task):
    data_dict: Requires[dict[str, Loader[Data]]]
    trained_model_path = DataPath('trained.bin')
    
    def init(self, preprocessed_data: Task[dict[str, Loader[Data]]], train_config: dict, seed: int):
        self.data_dict = preprocessed_data
        self.train_config = train_config
        self.seed = seed
    
    def main(self) -> Loader[Model]:
        train_data = self.data_dict['train'].load()
        valid_data = self.data_dict['valid'].load()
        # Train model with data and save it to trained_path ...
        return PickleLoader(Model('<trained model>'), self.trained_model_path)


@infer_task_type
class TestModel(Task):
    data_dict: Requires[dict[str, Loader[Data]]]
    model: Requires[Loader[Model]]

    def init(self, preprocessed_data: Task[dict[str, Loader[Data]]], trained_model: Task[Loader[Model]]):
        self.data_dict = preprocessed_data
        self.model = trained_model
    
    def main(self) -> dict[str, Any]:
        test_data = self.data_dict['test'].load()
        model = self.model.load()
        # Evaluate model on test_data ...
        result = {'score': ...}
        return result


@infer_task_type
class Main(Task):
    results: RequiresList[dict]

    def init(self):
        tasks: list[Task[dict]] = []
        for i in range(10):
            data = PreprocessData('mydata', split_ratio=.8, seed=i)
            trained = TrainModel(preprocessed_data=data, train_config={'lr': .01}, seed=i)
            result = TestModel(preprocessed_data=data, trained_model=trained)
            tasks.append(result)
        self.results = tasks

    def main(self) -> list[dict]:
        print('running main')
        return self.results
