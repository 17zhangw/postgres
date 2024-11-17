import os
from pathlib import Path
from doit.action import CmdAction

from dodos import VERBOSITY_DEFAULT

ROOT_FOLDER = Path(__file__).parent.parent.absolute()
ARTIFACT_config_log = (ROOT_FOLDER / "config.log").absolute()
ARTIFACT_postgres = (ROOT_FOLDER / "build/bin/postgres").absolute()


def task_np_config_clear():
    """
    NoisePage: Clear the old config file. THIS MUST BE DONE FOR np_config TO SWITCH BUILD TYPES.
    """
    return {
        "actions": [
            "rm -rf ./config.log ./config.status",
        ],
        "verbosity": VERBOSITY_DEFAULT,
        "uptodate": [False],
    }


def task_np_config():
    """
    NoisePage: Configure building in either debug or release mode.
    """
    return {
        "actions": [
            "./cmudb/build/configure.sh %(build_type)s",
        ],
        "targets": [ARTIFACT_config_log],
        "verbosity": VERBOSITY_DEFAULT,
        "uptodate": [True],
        "params": [
            {
                "name": "build_type",
                "long": "build_type",
                "help": 'Must be either "debug" or "release", defaults to "debug".',
                "default": "debug",
            },
        ],
    }


def task_np_clean():
    """
    NoisePage: Clean any previous NoisePage binary build.
    """
    return {
        "actions": [
            "make -j -s clean",
        ],
        "file_dep": [ARTIFACT_config_log],
        "verbosity": VERBOSITY_DEFAULT,
        "uptodate": [False],
    }


def task_np_build():
    """
    NoisePage: Build the NoisePage binary.
    """
    return {
        "actions": [
            "make -j -s install-world-bin",
        ],
        "file_dep": [ARTIFACT_config_log],
        "targets": [ARTIFACT_postgres],
        "verbosity": VERBOSITY_DEFAULT,
        "uptodate": [False],
    }


def task_np_install():
    """
    NoisePage: Build the NoisePage binary.
    """
    return {
        "actions": [
            "make install",
            "cp -r build/bin/* %(output)s",
        ],
        "file_dep": [ARTIFACT_config_log],
        "verbosity": VERBOSITY_DEFAULT,
        "uptodate": [False],
        "params": [
            {
                "name": "output",
                "long": "output",
                "help": 'Output',
                "default": "debug",
            },
        ],
    }


def task_np_build_extensions():
    path = os.environ["PATH"]
    npath = f"../../build/bin:{path}"
    return {
        "actions": [
            CmdAction("(cd cmudb/pg_hint_plan && make clean && make install)", env={"PATH": npath}),
            CmdAction("(cd cmudb/HypoPG && make clean && make install)", env={"PATH": npath}),
            CmdAction("(cd cmudb/hypocost && make clean && make install)", env={"PATH": npath}),
            CmdAction("(cd cmudb/pgvector && make clean && make install)", env={"PATH": npath}),
            CmdAction("(cd cmudb/boot_rs && cargo clean && cargo build --release && cbindgen . -o target/boot_rs.h --lang c)", env={"PATH": npath}),
            CmdAction("(cd cmudb/boot && make clean && make install)", env={"PATH": npath}),
        ],
        "file_dep": [ARTIFACT_postgres],
        "verbosity": VERBOSITY_DEFAULT,
        "uptodate": [False],
    }
