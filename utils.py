import inspect


def type_check(func):
    def check(*args, **kwargs):
        # TODO: doesn't support wildcard args and kwargs
        # TODO: assumed kwargs defined type is the same with default value
        signature_args = [param for param in inspect.signature(func).parameters.values()
                          if param.kind == param.POSITIONAL_OR_KEYWORD]

        # handle input parameters input positional arguments format
        # the input args is one of the three situations
        # 1. defined as a positional argument in func with a signature
        # 2. defined as a positional argument in func without a signature
        # 3. defined as a keyword argument in func but used as a positional argument
        args_invalid = [(signature_args[arg_idx], arg, signature_args[arg_idx].annotation)
                        for arg_idx, arg in enumerate(args)
                        if not(type(arg) == signature_args[arg_idx].annotation or
                               (inspect._empty == signature_args[arg_idx].annotation and
                                   inspect._empty == signature_args[arg_idx].default
                                ) or
                               (type(arg) == type(
                                   signature_args[arg_idx].default))
                               )
                        ]

        # handle input parameters as in keyword arguments format
        kwargs_invalid = [(kwarg, type(kwarg_value), type(inspect.signature(func).parameters[kwarg].default))
                          for kwarg, kwarg_value in kwargs.items()
                          if not (type(kwarg_value) == type(inspect.signature(func).parameters[kwarg].default))
                          ]

        # TODO: change exception handler here
        try:
            assert len(args_invalid + kwargs_invalid) == 0
        except:
            err_msg = ''
            for arg_name, input_type, _ in args_invalid + kwargs_invalid:
                err_msg += 'Parameter: {} received a wrong type. The input type is {}. \n'.format(
                    arg_name, type(input_type))
            raise ValueError(err_msg)
            

        return func(*args, **kwargs)

    return check


if __name__ == '__main__':
    @type_check
    def a(b: str, c, d=1) -> None:
        return None

    a('1', '2', 'c')
    print('expected: d has a wrong type', '\n')

    a('1', '2', 0.0)
    print('expected: d has a wrong type', '\n')

    a('1', '2', int(1))
    print('expected: correct types', '\n')

    a('1', '2', d=int(1))
    print('expected: correct types', '\n')

    a('1', 2, d=int(1))
    print('expected: correct types', '\n')

    a(1, '2', 'c')
    print('expected: b has a wrong type and d has a wrong type', '\n')
