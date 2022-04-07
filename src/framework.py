from ast import Call
import sys
import inspect
from textwrap import wrap
from types import ModuleType
from typing import Callable, List


def data_source(source_func: Callable) -> Callable:
    def wrapped_source(*args, **kwargs):
        result = source_func(*args, **kwargs)
        result.printSchema()
        result.show()
        return result

    return wrapped_source


def data_sink(sink_func: Callable) -> Callable:
    def wrapped_sink(result):
        result.printSchema()
        result.show()
        return sink_func(result)

    return wrapped_sink


# Decorator args are addtional arguments here
def data_calculator(**kwargs) -> Callable:
    sink: Callable = kwargs['sink']
    def wrap_calculator_func(calc_func):
        def wrapped_calculator(*args, **kwargs):
            result = calc_func(*args, **kwargs)
            print('Got result')
            sink(result)
            return result
    
        return wrapped_calculator

    return wrap_calculator_func


def _is_calculator_function(func: object) -> bool:
    if not inspect.isfunction(func):
        return False

    if func.__name__ == 'create_foods_dataset':
        print(func)

    return True


def _find_output_functions(job_module: ModuleType) -> List[Callable]:
    return [f for (_, f) in inspect.getmembers(job_module) if _is_calculator_function(f)]


def run_with_local_spark(module_name: str):
    print('Running with local spark', module_name)
    job_module = sys.modules[module_name]
    print('module', job_module)

    output_functions = _find_output_functions(job_module)
    for out_func in output_functions:
        print(out_func)
