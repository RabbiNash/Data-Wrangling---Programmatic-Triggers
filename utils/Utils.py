import subprocess


class Utils(object):

    @staticmethod
    def install(name):
        subprocess.call(['pip', 'install', name])

    @staticmethod
    def lookahead(iterable):
        """Pass through all values from the given iterable, augmented by the
        information if there are more values to come after the current one
        (True), or if it is the last value (False).
        """
        # Get an iterator and pull the first value.
        it = iter(iterable)
        last = next(it)
        # Run the iterator to exhaustion (starting from the second value).
        for val in it:
            # Report the *previous* value (more to come).
            yield last, True
            last = val
        # Report the last value.
        yield last, False

    # try:
    #     import pandas as pd
    # except ImportError:
    #     subprocess.check_call([sys.executable, "-m", "pip", "install", 'pandas'])
    # finally:
    #     import pandas as pd
