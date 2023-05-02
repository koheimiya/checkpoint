from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, NewType, Protocol, TypeVar

from cloudpickle import dump, load
from checkpoint import Task, task, requires, TaskDirectory


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


@task(queue='data')
def load_data(name: str):

    @requires(TaskDirectory())
    def __(path: Path) -> Loader[Data]:
        # Download a dataset ...
        data_loader = PickleLoader(Data(f'<data: {name}>'), path / f'data.txt')
        return data_loader
    return __


@task
def preprocess_data(name: str, split_ratio: float, seed: int):

    @requires(TaskDirectory())
    @requires(load_data(name))
    def __(path: Path, data_loader: Loader[Data]) -> dict[str, Loader[Data]]:
        data = data_loader.load()
        # Split the dataset at data_path into splits ...
        train_loader = PickleLoader(Data('<train data>'), path / 'train.txt')
        valid_loader = PickleLoader(Data('<valid data>'), path / 'train.txt')
        test_loader = PickleLoader(Data('<test data>'), path / 'train.txt')
        return {'train': train_loader, 'valid': valid_loader, 'test': test_loader}
    return __


@task
def load_model(name: str):

    @requires(TaskDirectory())
    def __(path: Path) -> Loader[Model]:
        model_path = path / f'{name}.bin'
        # Download the model to model_path ...
        return PickleLoader(Model('<initial model>'), model_path)
    return __


@task
def train_model(preprocessed_data: Task[dict[str, Loader[Data]]], initial_model: Task[Loader[Model]], train_config: dict, seed: int):
    
    @requires(TaskDirectory())
    @requires(preprocessed_data)
    @requires(initial_model)
    def __(path: Path, data_dict: dict[str, Loader[Data]], model_loader: Loader[Model]) -> Loader[Model]:
        train_data = data_dict['train'].load()
        valid_data = data_dict['valid'].load()
        model = model_loader.load()
        # Train model with data and save it to trained_path ...
        trained_path = path / f'trained.bin'
        return PickleLoader(Model('<trained model>'), trained_path)
    return __


@task
def test_model(preprocessed_data: Task[dict[str, Loader[Data]]], trained_model: Task[Loader[Model]]):
    
    @requires(preprocessed_data)
    @requires(trained_model)
    def __(dataset: dict[str, Loader[Data]], model_loader: Loader[Model]) -> dict[str, Any]:
        test_data = dataset['test'].load()
        model = model_loader.load()
        # Evaluate model on test_data ...
        result = {'score': ...}
        return result
    return __


@task
def main():

    tasks: list[Task[dict]] = []
    for i in range(10):
        data = preprocess_data('mydata', split_ratio=.8, seed=i)
        model = load_model('mymodel')
        trained = train_model(preprocessed_data=data, initial_model=model, train_config={'lr': .01}, seed=i)
        result = test_model(preprocessed_data=data, trained_model=trained)
        tasks.append(result)

    @requires(tasks)
    def __(results: list[dict]) -> list[dict]:
        return results
    return __
