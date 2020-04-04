import inspect
import shutil
import __main__

from pathlib import Path


def get_methods_with_decorator(cls, decorator_name):
    source_lines = inspect.getsourcelines(cls)[0]
    for i, line in enumerate(source_lines):
        line = line.strip()
        if line.split('(')[0].strip() == '@' + decorator_name:  # leaving a bit out
            next_line = source_lines[i+1]
            name = next_line.split('def')[1].split('(')[0].strip()
            yield(name)


def get_python_path(o):
    # o.__module__ + "." + o.__class__.__qualname__ is an example in
    # this context of H.L. Mencken's "neat, plausible, and wrong."
    # Python makes no guarantees as to whether the __module__ special
    # attribute is defined, so we take a more circumspect approach.
    # Alas, the module name is explicitly excluded from __qualname__
    # in Python 3.

    module = o.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return o.__class__.__name__  # Avoid reporting __builtin__
    elif module == __main__.__name__:
        module_path = Path(__main__.__file__).relative_to(Path.cwd())
        return module_path.as_posix().replace('/', '.').replace(module_path.suffix, '') + '.' + o.__class__.__name__
    else:
        return module + '.' + o.__class__.__name__
