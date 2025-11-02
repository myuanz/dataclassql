def unwarp_or[T](x: T|None, default: T) -> T:
    if x is None:
        return default
    return x

def unwarp_or_raise[T](x: T|None, exc: Exception) -> T:
    if x is None:
        raise exc
    return x

def unwarp[T](x: T|None) -> T:
    return unwarp_or_raise(x, ValueError('Value is None'))
