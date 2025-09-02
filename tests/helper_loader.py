
from importlib.machinery import SourceFileLoader
from importlib.util import spec_from_loader, module_from_spec

def load_module_from_path(mod_name: str, path: str):
    loader = SourceFileLoader(mod_name, path)
    spec = spec_from_loader(mod_name, loader)
    module = module_from_spec(spec)
    loader.exec_module(module)
    return module
