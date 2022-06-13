import importlib
import inspect
import pkgutil
import six

__all__ = []

for loader, name, is_pkg in pkgutil.walk_packages(__path__):
    if six.PY2:
        module = loader.find_module(name).load_module(name)
    else:
        spec = loader.find_spec(name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

    for name, entry in inspect.getmembers(module):
        if name.startswith('handle_') or name.startswith('anon_handle_'):
            globals()[name] = entry
            __all__.append(name)
