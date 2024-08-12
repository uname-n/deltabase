from shutil import rmtree
from os.path import exists
from os import remove

def pytest_addoption(parser):
    parser.addoption("--width", action="store", default=100)
    parser.addoption("--height", action="store", default=100)

def pytest_generate_tests(metafunc):
    option_value = metafunc.config.option.width
    if 'width' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("width", [int(option_value)])

    option_value = metafunc.config.option.height
    if 'height' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("height", [int(option_value)])

def pytest_sessionfinish(session, exitstatus):
    if exists("test.delta"): rmtree("test.delta")
    if exists("test.db"): remove("test.db")
