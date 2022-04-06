import sys

def run_with_local_spark(module_name: str):
    print('Running with local spark', module_name)
    job_module = sys.modules[module_name]
    print('module', job_module)