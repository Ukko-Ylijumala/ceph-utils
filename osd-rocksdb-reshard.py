#!/usr/bin/env python3

"""
osd-rocksdb-reshard.py - A script to reshard RocksDB databases in Ceph OSDs.
"""

__author__    = "Mikko Tanner"
__copyright__ = f"(c) {__author__} 2025"
__version__   = "0.1.0-1_20250722"
__license__   = "GPL-3.0-or-later"

import glob
import os
import sys
from argparse import ArgumentParser
from subprocess import run, CalledProcessError, TimeoutExpired
from time import time, sleep
from typing import Dict, Iterable, List, Optional, NoReturn
from re import compile as re_compile, error as re_error, Pattern

ANSI_RX   = re_compile(r'\x1b\[[0-9;]*m')   # matches standard ANSI color escape sequences
HOSTNAME  = os.uname().nodename
DRYRUN    = False  # set to True to enable dry-run mode
UNIT_BASE = 'ceph-osd@'
OSD_BASE  = '/var/lib/ceph/osd'
ERR_NO_SHARDING = 'failed to retrieve sharding def'
DEFAULT = 'm(3) p(3,0-12) O(3,0-13)=block_cache={type=binned_lru} \
    L=min_write_buffer_number_to_merge=32 P=min_write_buffer_number_to_merge=32'


def parse_cmdline_args():
    """Parse command-line arguments."""
    args = ArgumentParser(description='OSD RocksDB resharding tool')
    args.add_argument('osds', nargs='*', type=int,
                      help='List of OSD IDs to reshard (default: all OSDs)')
    args.add_argument('--cluster', default='ceph', help='Cluster name (default: ceph)')
    args.add_argument('--timeout', type=int, default=60,
                      help='Timeout for OSD systemctl operations in seconds (default: 60)')
    args.add_argument('--yes', action='store_true', help='Assume yes to all prompts')
    mode = args.add_mutually_exclusive_group()
    mode.add_argument('--force', action='store_true',
                      help='Force resharding even if it is not needed')
    mode.add_argument('--dryrun', action='store_true',
                      help='Do not perform any changes, just show what would be done')
    args.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    p = args.parse_args()

    if not p.osds:
        # if no OSDs are specified, assume all OSDs
        p.osds = [int(os.path.basename(d).split('-')[1])
                  for d in glob.glob(f'{OSD_BASE}/{p.cluster}-*')]
        if not p.osds:
            bailout(f'no OSDs found in {OSD_BASE}')
    else:
        # validate that all specified OSDs exist
        for osd in p.osds:
            data_path = osd_libpath(osd, cluster=p.cluster)
            if not os.path.exists(data_path):
                bailout(f"no data directory for OSD {osd} in '{OSD_BASE}'")
            elif not os.path.isdir(data_path):
                bailout(f"'{data_path}' is not a directory")
            elif not os.listdir(data_path):
                bailout(f"OSD {osd} data directory '{data_path}' is empty")

    if p.dryrun:
        global DRYRUN
        DRYRUN = True
        INFO('dry-run mode enabled. No changes will be made.')

    return p


def _CC(*args):
    """Make an ANSI control code string."""
    return f"\033[{';'.join(str(i) for i in args)}m"

def RED(_s: str):
    """Make a string bold red."""
    return f'{_CC(91, 1)}{_s}{_CC(0)}'

def GRN(_s: str):
    """Make a string bold green."""
    return f'{_CC(32, 1)}{_s}{_CC(0)}'

def YEL(_s: str):
    """Make a string bold yellow."""
    return f'{_CC(33, 1)}{_s}{_CC(0)}'

def BOLD(_s: str):
    """Make a string bold."""
    return f'{_CC(1)}{_s}{_CC(0)}'

def FAINT(_s: str):
    """Make a string faint (dimmed)."""
    return f'{_CC(2)}{_s}{_CC(0)}'


def eprint(*values, **kwargs):
    """Mimic print() but write to stderr."""
    print(*values, file=sys.stderr, **kwargs)


def bailout(msg: str, exit_code = 1, prepend = True) -> NoReturn:
    """
    Print a (error) message and exit with the given exit code.

    Args:
        msg: The (error) message to print
        exit_code: The exit code to use (default: 1)
        prepend: If True, prepend the message with 'ERROR: '
    """
    eprint(f'ERROR: {msg}' if prepend else msg)
    sys.exit(exit_code)


def INFO(msg: str):
    """Print an informational message to stderr, with INFO: prepended."""
    eprint(GRN(f'INFO: {msg}'))


def WARN(msg: str):
    """Print a warning message to stderr, with WARN: prepended."""
    eprint(YEL(f'WARN: {msg}'))


def simple_tabulate(data: Iterable[Iterable], headers: Iterable = None, missing = '-'):
    """
    Format a list of iterables as a table for printing.

    Args:
        data: List of iterables (lists, tuples) containing the data to display
        headers: Optional list of column headers
        missing: String to replace None values with

    Returns:
        String containing the formatted table
    """
    def visible_len(s: str) -> int:
        """Return the visible length of a string, ignoring ANSI escape codes."""
        return len(ANSI_RX.sub('', s))

    def format_row(row: tuple[str], widths: List[int]):
        """Format a single row (with padding if needed)."""
        items = []
        for i, item in enumerate(row):
            vis_len = visible_len(item)
            pad = ' ' * (widths[i] - vis_len)
            items.append(item + pad)
        diff = len(widths) - len(items)
        if diff > 0:
            # pad with `missing` value(s) if the row is too short
            missing_vis_len = visible_len(missing)
            for j in range(diff):
                pad = ' ' * (widths[len(items) + j] - missing_vis_len)
                items.append(missing + pad)
        return ' | '.join(items)

    all_rows: List[tuple[str]] = []
    if headers is not None:
        all_rows.append(tuple(str(h) for h in headers))
    for row in data:
        all_rows.append(tuple(str(item) if item is not None else missing for item in row))
    if not all_rows:
        return ''

    # Find the maximum width needed for each column (based on visible lengths)
    columns = 1
    for row in all_rows:
        columns = max(len(row), columns)
    widths = [0] * columns
    for row in all_rows:
        for i, item in enumerate(row):
            widths[i] = max(widths[i], visible_len(item))

    # Format each row with appropriate padding
    formatted_rows: List[str] = []
    if headers is not None:
        # Format headers with a separator line if headers are provided
        formatted_rows.append(format_row(all_rows[0], widths))
        separator = '-+-'.join('-' * w for w in widths)
        formatted_rows.append(separator)

    for row in all_rows[1:] if headers else all_rows:
        formatted_rows.append(format_row(row, widths))

    return '\n'.join(formatted_rows)


def run_cmd(cmd: str, cmd_args: Optional[List[str]] = None, timeout: Optional[int] = None):
    """Run a command with args with subprocess.run and return the result `(stdout, stderr)`."""
    if not cmd_args:
        cmd_args = []
    elif not isinstance(cmd_args, list):
        cmd_args = [cmd_args]
    full = [cmd] + cmd_args

    if DRYRUN:
        # in dry-run mode, just print the command and return bogus output
        eprint(YEL(f"*** would run: {BOLD(' '.join(full))}"))
        return 'dryrun', 'dryrun'

    try:
        res = run(full, capture_output=True, text=True, check=True, timeout=timeout)
        # make sure we always return strings
        out = res.stdout.strip() if res.stdout else ''
        err = res.stderr.strip() if res.stderr else ''
        return out, err
    except CalledProcessError as e:
        err = f"failed to run '{cmd}': {e}"
        eprint(RED(err))
        raise RuntimeError(err) from e
    except TimeoutExpired as e:
        err = f"command '{cmd}' timed out after {timeout} seconds"
        eprint(RED(err))
        raise TimeoutError(err) from e


def osd_libpath(osd: int, cluster: str):
    """Construct an OSD's data directory path (in /var/lib/ceph)"""
    return os.path.join(OSD_BASE, f'{cluster}-{osd}')


def systemctl_op(op: str, osd: int):
    """
    Perform a systemctl operation on a given unit.

    Args:
        op: The systemctl operation (e.g., 'start', 'stop', 'restart', 'is-active'...)
        osd_id: The OSD ID to operate on
    """
    unit_name = f'{UNIT_BASE}{osd}.service'
    try:
        return run_cmd('systemctl', [op, unit_name])[0]
    except (RuntimeError, TimeoutError) as e:
        bailout(f"'systemctl {op}' failed for {unit_name}: {e}")


def is_osd_active(osd: int, cluster: str):
    """Check if a given OSD (service) is active."""
    systemd_status_up = systemctl_op('is-active', osd) == 'active'
    # we don't fully trust systemd here, so we also check the admin socket
    try:
        with open(f'/var/run/ceph/{cluster}-osd.{osd}.asok', 'r', encoding='utf-8'):
            # if the socket can be opened, the OSD is likely running
            if not systemd_status_up:
                WARN(f'systemd claims OSD {osd} is down, but admin socket exists.')
            return True
    except FileNotFoundError:
        # if the socket file does not exist, the OSD is not running
        if systemd_status_up:
            WARN(f'systemd claims OSD {osd} is up, but no admin socket exists.')
        return False


def wait_till_inactive(osd_id: int, cluster: str, timeout = 60):
    """
    Wait until the OSD service is inactive.

    Args:
        osd_id: The OSD ID to check
        timeout: Maximum time to wait in seconds (default: 60)
    """
    if DRYRUN:
        eprint(YEL(f"*** would wait for OSD {osd_id} to become inactive (dry-run mode)"))
        return

    start_time = time()
    while True:
        if not is_osd_active(osd_id, cluster):
            elapsed = time() - start_time
            INFO(f'OSD {osd_id} was shut down after {elapsed:.1f}s')
            break
        if time() - start_time > timeout:
            bailout(f'timeout after {timeout}s waiting for OSD {osd_id} to shut down')
        sleep(1)


def get_sharding_def(osd_id: int, cluster: str) -> Optional[str]:
    """
    Get the RocksDB sharding definition for a given OSD.

    Args:
        osd_id: The OSD ID to query
        cluster: The name of the Ceph cluster

    Returns:
        The sharding definition string, or None for no sharding.
    """
    if is_osd_active(osd_id, cluster) and not DRYRUN:
        bailout(f'OSD {osd_id} is active, cannot read sharding definition.')

    try:
        osd_path = osd_libpath(osd_id, cluster)
        result = run_cmd('ceph-bluestore-tool', ['show-sharding', '--path', osd_path])
        if ERR_NO_SHARDING in (result[0], result[1]) or DRYRUN:
            # this is a legacy OSD without RocksDB sharding
            return None
        return result
    except RuntimeError as e:
        bailout(f'could not get sharding for OSD {osd_id}: {e}')


def reshard(osd_id: int, cluster: str, sharding = DEFAULT) -> None:
    """
    Reshard the RocksDB for a given OSD device.

    Args:
        osd_id: The OSD ID to reshard
        cluster: The name of the Ceph cluster
        sharding: The RocksDB sharding definition to apply
    """
    if is_osd_active(osd_id, cluster) and not DRYRUN:
        bailout(f'OSD {osd_id} is active, cannot reshard its RocksDB.')

    INFO(f"resharding OSD {osd_id} RocksDB with '{sharding}'")
    try:
        osd_path = osd_libpath(osd_id, cluster)
        run_cmd('ceph-bluestore-tool', ['reshard', '--path', osd_path, '--sharding', sharding])
        INFO(f'resharding finished for OSD {osd_id}')
    except RuntimeError as e:
        bailout(f'failed to reshard OSD {osd_id}: {e}')


def main():
    args = parse_cmdline_args()
    if not (args.yes or args.force):
        WARN(f"this script will reshard OSDs (cluster '{args.cluster}'): {args.osds}")
        eprint(' >>> The operation may take a while and could even cause a loss of OSD(s).')
        eprint(' >>> Each OSD will be stopped, resharded, and restarted in sequence.')
        eprint(f"(Use '{os.path.basename(__file__)} --yes' to skip this prompt.)")
        if input(RED('Are you really sure you want to proceed? (y/N): ')).strip().lower() != 'y':
            bailout('Aborting.', exit_code=0, prepend=False)

    for osd in args.osds:
        # stop the OSD service
        if not DRYRUN:
            INFO(f'stopping OSD {osd}...')
            systemctl_op('stop', osd=osd)
        wait_till_inactive(osd, args.cluster)

        sharding_def = get_sharding_def(osd, args.cluster)
        if sharding_def is None:
            INFO(f'OSD {osd} has no RocksDB sharding -> initialize sharding')
            reshard(osd, cluster=args.cluster, sharding=DEFAULT)
        elif args.force:
            WARN(f'OSD {osd} RocksDB already is sharded, but --force is set: {sharding_def}')
            reshard(osd, cluster=args.cluster, sharding=DEFAULT)
        else:
            INFO(f'OSD {osd} already has RocksDB sharding - skipping: {sharding_def}')

        # restart the OSD service
        if not DRYRUN:
            INFO(f'restarting OSD {osd}...')
            systemctl_op('start', osd=osd)
            INFO(f'waiting for OSD {osd} to become active...')
            while not is_osd_active(osd, args.cluster):
                sleep(1)
            INFO(f'OSD {osd} restarted successfully')

    INFO('All done.')


if __name__ == "__main__":
    main()
