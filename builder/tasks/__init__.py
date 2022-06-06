import pkgutil
import importlib
import inspect
import koji

__all__ = []

# py3 only
for loader, name, is_pkg in pkgutil.walk_packages(__path__):
    spec = loader.find_spec(name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for name, entry in inspect.getmembers(module):
        if isinstance(entry, type(koji.tasks.BaseTaskHandler)) and \
                issubclass(entry, koji.tasks.BaseTaskHandler):
            globals()[name] = entry
            __all__.append(name)
