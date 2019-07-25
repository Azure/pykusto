from types import FunctionType


def stringify_python_func(func: FunctionType):
    return """from types import CodeType\\ncode=CodeType({},{},{},{},{},{},{},{},{},{},{},{},{},{},{})\\nexec(code)\\n""".format(
        func.__code__.co_argcount,
        func.__code__.co_kwonlyargcount,
        func.__code__.co_nlocals,
        func.__code__.co_stacksize,
        func.__code__.co_flags,
        func.__code__.co_code,
        func.__code__.co_consts,
        func.__code__.co_names,
        func.__code__.co_varnames,
        "'{}'".format(func.__code__.co_filename),
        "'{}'".format(func.__code__.co_name),
        func.__code__.co_firstlineno,
        func.__code__.co_lnotab,
        func.__code__.co_freevars,
        func.__code__.co_cellvars
    ).replace("\\x", "\\\\x")
