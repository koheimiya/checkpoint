# Checkpoint-tool
`functools.cache` becomes persistent, and more:
- Support JSON-serializable arguments
- Automatic dependency management (i.e., discard cache if upstream cache is altered/deleted)


### Installation
```
pip install .
```


### Usage
Drop-in replacement of `functools.cache`:
```python
from checkpoint import checkpoint

@checkpoint()
def choose(n: int, k: int) -> int:
    """ Compute binomial coefficient """
    if 0 < k < n:
        return choice(n - 1, k - 1) + choice(n - 1, k)  # Recursive call is nicely handled. 
    elif k == 0 or k == n:
        return 1
    else:
        raise ValueError(f'n={n}, k={k}')
        
ans = choose(6, 3)  # cache will be stored at $CHECKPOINT_DIR/.checkpoint/choice.sqlite
```

It is possible to selectively discard cache: 
```python
### after some modificaiton of choose(3, 3) ...
choose.delete(3, 3) # selectively discard the cache corresponding to the modification
ans = choose(6, 3)  # ans is recomputed tracing back to the computation of choose(3, 3)
choose.clear()      # delete all cache
```

More complex inputs can be used as long as it is JSON serializable:
```python
@checkpoint()
def task1(**param1):
    ...

@checkpoint()
def task2(**param2):
    ...

@checkpoint()
def task3(params):
    result1 = task1(**params['param1'])
    result2 = task2(**params['param2'])
    ...

result = task3({'param1': { ... }, 'param2': { ... }})
```

Large outputs can be stored with compression:
```python
@checkpoint(compress=True)
def large_result_computation(*args, **kwargs):
    ...
```
