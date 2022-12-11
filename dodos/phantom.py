import os

import doit

from dodos import VERBOSITY_DEFAULT


def task_phantom_install():
    """
    Phantom: Compile and install the phantom extension.
    """
    return {
        "actions": [
            lambda: os.chdir("cmudb/phantom/"),
            # Generate the necessary features.
            "PG_CONFIG=%(pg_config)s make clean -j",
            "PG_CONFIG=%(pg_config)s make -j",
            "PG_CONFIG=%(pg_config)s make install -j",
            # Reset working directory.
            lambda: os.chdir(doit.get_initial_workdir()),
        ],
        "verbosity": VERBOSITY_DEFAULT,
        "uptodate": [False],
        "params": [
            {
                "name": "pg_config",
                "long": "pg_config",
                "help": "The location of the pg_config binary.",
                "default": "../../build/bin/pg_config",
            },
        ],
    }
