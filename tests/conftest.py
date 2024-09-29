from shutil import rmtree
from os.path import exists

def pytest_sessionfinish(session, exitstatus):
    if exists("test.delta"): rmtree("test.delta")