from pathlib import Path
from typing import Any
from checkpoint import Task, taskflow, requires, TaskDirectory


@taskflow
def load_data(name: str):

    @requires(TaskDirectory())
    def __(path: Path) -> Path:
        data_path = path / f'data.txt'
        # Download the dataset to data_path ...
        open(data_path, 'w').write('<some data>')
        return data_path
    return __


@taskflow
def preprocess_data(name: str, split_ratio: float, seed: int):

    @requires(TaskDirectory())
    @requires(load_data(name))
    def __(path: Path, data_path: Path) -> tuple[Path, Path, Path]:
        train_path = path / f'train.txt'
        valid_path = path / f'valid.txt'
        test_path = path / f'test.txt'
        # Split the dataset at data_path into splits ...
        open(train_path, 'w').write('<train data>')
        open(valid_path, 'w').write('<valid data>')
        open(test_path, 'w').write('<test data>')
        return train_path, valid_path, test_path
    return __


@taskflow
def load_model(name: str):

    @requires(TaskDirectory())
    def __(path: Path) -> Path:
        model_path = path / f'{name}.bin'
        # Download the model to model_path ...
        open(model_path, 'w').write('<initial model>')
        return model_path
    return __


@taskflow
def train_model(preprocessed_data: Task[tuple[Path, Path, Path]], initial_model: Task[Path], train_config: dict, seed: int):
    
    @requires(TaskDirectory())
    @requires(preprocessed_data)
    @requires(initial_model)
    def __(path: Path, train_valid_test: tuple[Path, Path, Path], model_path: Path) -> Path:
        train, valid, _ = train_valid_test
        train_data = open(train, 'r').read()
        valid_data = open(valid, 'r').read()
        model = open(model_path, 'r').read()
        trained_path = path / f'trained.bin'
        # Train model with data and save it to trained_path ...
        open(trained_path, 'w').write('<trained model>')
        return trained_path
    return __


@taskflow
def test_model(preprocessed_data: Task[tuple[Path, Path, Path]], trained_model: Task[Path]):
    
    @requires(preprocessed_data)
    @requires(trained_model)
    def __(train_valid_test: tuple[Path, Path, Path], model_path: Path) -> dict[str, Any]:
        _, _, test = train_valid_test
        test_data = open(test, 'r').read()
        model = open(model_path, 'r').read()
        # Evaluate model on test_data ...
        result = {'score': ...}
        return result
    return __


@taskflow
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
