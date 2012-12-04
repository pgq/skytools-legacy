# handlers module
import new
import sys

DEFAULT_HANDLERS = [
    'londiste.handlers.qtable',
    'londiste.handlers.applyfn',
    'londiste.handlers.part',
    'londiste.handlers.multimaster',
    'londiste.handlers.vtable',

    'londiste.handlers.bulk',
    'londiste.handlers.dispatch',
]

def handler_args(name, cls):
    """Handler arguments initialization decorator

    Define successor for handler class cls with func as argument generator
    """
    def wrapper(func):
        def _init_override(self, table_name, args, dest_table):
            cls.__init__(self, table_name, func(args.copy()), dest_table)
        dct = {'__init__': _init_override, 'handler_name': name}
        module = sys.modules[cls.__module__]
        newname = '%s_%s' % (cls.__name__, name.replace('.','_'))
        newcls = new.classobj(newname, (cls,), dct)
        setattr(module, newname, newcls)
        module.__londiste_handlers__.append(newcls)
        module.__all__.append(newname)
        return func
    return wrapper

def update(*p):
    """ Update dicts given in params with its predecessor param dict
    in reverse order """
    return reduce(lambda x, y: x.update(y) or x,
            (p[i] for i in range(len(p)-1,-1,-1)), {})
