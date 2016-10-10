
def _require(*kwargs):
    def get_decorator(func):
        def do_func(obj, var, **kwds):
            for kw in kwargs:
                if kw not in kwds:
                    # if in_ipynb():
                    #   widgets.widget_string.Text()
                    # else:
                    arg = raw_input('{}: '.format(kw))
                kwds[kw] = arg

            func(obj, var, **kwds)
        return do_func
    return get_decorator