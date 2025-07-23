#!/usr/bin/env python3

"""
osd-rocksdb-reshard.py - A script to reshard RocksDB databases in Ceph OSDs.
"""

__author__    = "Mikko Tanner"
__copyright__ = f"(c) {__author__} 2025"
__version__   = "0.1.5-1_20250723"
__license__   = "GPL-3.0-or-later"

import errno
import fcntl
import glob
import os
import sys
from argparse import ArgumentParser
from subprocess import run, CalledProcessError, TimeoutExpired
from time import time, sleep
from typing import List, Optional, NoReturn

HOSTNAME  = os.uname().nodename
DRYRUN    = False  # set to True to enable dry-run mode
UNIT_BASE = 'ceph-osd@'
OSD_BASE  = '/var/lib/ceph/osd'
ERR_NO_SHARDING = 'failed to retrieve sharding def'
DEFAULT = """m(3) p(3,0-12) O(3,0-13)=block_cache={type=binned_lru} \
L=min_write_buffer_number_to_merge=32 P=min_write_buffer_number_to_merge=32"""


def parse_cmdline_args():
    """Parse command-line arguments."""
    args = ArgumentParser(description='OSD RocksDB resharding tool')
    args.add_argument('osds', nargs='*', type=int,
                      help='List of OSD IDs to reshard (default: all OSDs)')
    args.add_argument('--exclude', '-E', default=set(),
                      help='Comma-separated list of OSD IDs to exclude (default: none)')
    args.add_argument('--cluster', default='ceph', help='Cluster name (default: ceph)')
    args.add_argument('--timeout', type=int, default=60,
                      help='Timeout for OSD systemctl operations in seconds (default: 60)')
    args.add_argument('--yes', action='store_true', help='Assume yes to all prompts')
    args.add_argument('--fsck', action='store_true', help='Run fsck on each OSD before resharding')
    MODE = args.add_mutually_exclusive_group()
    MODE.add_argument('--force', action='store_true',
                      help='Force resharding even if it is not needed')
    MODE.add_argument('--dryrun', action='store_true',
                      help='Do not perform any changes, just show what would be done')
    args.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    p = args.parse_args()

    # validate exclusions and remove duplicates
    if isinstance(p.exclude, str):
        if not ',' in p.exclude and not p.exclude.strip().isdigit():
            bailout(f"invalid exclusion '{p.exclude}': must be a comma-separated list of OSD IDs")
        # split exclusions string by comma and convert elements to int
        p.exclude = set(map(int, (i.strip() for i in p.exclude.split(',') if i.strip().isdigit())))

    if not p.osds:
        # if no OSDs are specified, assume all valid OSDs
        p.osds = [int(os.path.basename(d).split('-')[1])
                  for d in glob.glob(f'{OSD_BASE}/{p.cluster}-*')
                  if os.path.isdir(d) and
                     'fsid' in os.listdir(d) and
                     os.path.isfile(os.path.join(d, 'fsid'))]
        if not p.osds:
            bailout(f'no OSDs found in {OSD_BASE}')
        p.osds = list(set(p.osds) - p.exclude)
    else:
        # validate that all specified OSDs exist and only appear once
        p.osds = list(set(p.osds) - p.exclude)
        for osd in p.osds:
            data_path = osd_libpath(osd, cluster=p.cluster)
            if not os.path.exists(data_path):
                bailout(f"no data directory for OSD {osd} in '{OSD_BASE}'")
            elif not os.path.isdir(data_path):
                bailout(f"'{data_path}' is not a directory")
            elif not os.listdir(data_path):
                bailout(f"OSD {osd} data directory '{data_path}' is empty")

    # check and sort OSDs for consistent ordering
    if not p.osds:
        bailout('no OSDs left to reshard after exclusion.')
    p.osds.sort()

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
    eprint(f"{RED('ERROR')}: {msg}" if prepend else msg)
    sys.exit(exit_code)


def INFO(msg: str):
    """Print an informational message to stderr, with INFO: prepended."""
    eprint(GRN(f'INFO: {msg}'))


def WARN(msg: str):
    """Print a warning message to stderr, with WARN: prepended."""
    eprint(YEL(f'WARN: {msg}'))


def run_cmd(cmd: str, cmd_args: Optional[List[str]] = None,
            timeout: Optional[int] = None, check = True):
    """Run a command with args with subprocess.run and return the result `(stdout, stderr)`."""
    if not cmd_args:
        cmd_args = []
    elif not isinstance(cmd_args, list):
        cmd_args = [cmd_args]
    full = [cmd] + cmd_args

    if DRYRUN:
        # in dry-run mode, just print the command and return bogus output
        eprint(YEL('*** would run:'), FAINT(' '.join(full)))
        return 'dryrun', 'dryrun'

    try:
        res = run(full, capture_output=True, text=True, check=check, timeout=timeout)
        # make sure we always return strings
        out = res.stdout.strip() if res.stdout else ''
        err = res.stderr.strip() if res.stderr else ''
        return out, err
    except CalledProcessError as e:
        err = f"run_cmd('{cmd}') failed: {e}"
        eprint(RED(err))
        raise RuntimeError(err) from e
    except TimeoutExpired as e:
        err = f"command '{cmd}' timed out after {timeout} seconds"
        eprint(RED(err))
        raise TimeoutError(err) from e


def osd_libpath(osd: int, cluster: str):
    """Construct an OSD's data directory path (in /var/lib/ceph)"""
    return os.path.join(OSD_BASE, f'{cluster}-{osd}')


def is_ceph_healthy(cluster: str):
    """
    Check whether the Ceph cluster is healthy (`HEALTH_OK` status).

    Also acceptable is `HEALTH_WARN` with `noout` flag (`HEALTH_WARN noout flag(s) set`).
    """
    try:
        out, _ = run_cmd('ceph', ['--cluster', cluster, 'health', 'detail'])
        # we only want the 1st line of (possibly multi-line) output
        state = out.splitlines()[0].strip()
        if state == 'HEALTH_OK':
            return True
        if state == 'HEALTH_WARN noout flag(s) set':
            # noout flag is set, which is the only case when we allow HEALTH_WARN
            # this is acceptable, as it means we can still safely bring OSDs down
            return True
        return False
    except RuntimeError as e:
        bailout(f'failed to check Ceph health: {e}')


def systemctl_op(op: str, osd: int, check = True):
    """
    Perform a systemctl operation on a given unit.

    Args:
        op: The systemctl operation (e.g., 'start', 'stop', 'restart', 'is-active'...)
        osd: The OSD ID to operate on
        check: If True, raise an exception on non-zero exit code
    """
    unit_name = f'{UNIT_BASE}{osd}.service'
    try:
        return run_cmd('systemctl', [op, unit_name], check=check)[0]
    except (RuntimeError, TimeoutError) as e:
        bailout(f"'systemctl {op}' failed for {unit_name}: {e}")


def is_osd_active(osd: int, cluster: str):
    """Check if a given OSD (service) is active."""
    # For some moronic reason, 'systemctl is-active' exits with code=3 if a unit is in 'inactive'
    # state. We need to disable the exit code check, or it will raise an exception, which is not
    # what we want (we're checking against the stdout string value instead).
    # See: https://bugs.freedesktop.org/show_bug.cgi?id=77507
    systemd_status_up = systemctl_op('is-active', osd, check=False) == 'active'

    # We don't fully trust systemd, so we also check an exclusive lock on the OSD's fsid file.
    # This replicates the logic from Ceph to determine if an OSD is already operational.
    fsid_path = os.path.join(osd_libpath(osd, cluster), 'fsid')
    lock_held = False
    if os.path.exists(fsid_path):
        try:
            fd = open(fsid_path, 'r+', encoding='utf-8')    # we need a writable fd for locking
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # non-blocking exclusive lock
            fcntl.lockf(fd, fcntl.LOCK_UN)                  # if acquired, release immediately
            fd.close()
        except OSError as e:
            if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):    # lock held by another process
                lock_held = True
            elif e.errno == errno.EBADF:
                bailout(f"can't lock {fsid_path}: bad file descriptor (check permissions or mode)")
            else:
                raise       # other errors (e.g., EACCES for permissions) should propagate
        except Exception:   #pylint: disable=broad-except
            # if any other issue (e.g., file vanished), assume not active but log
            WARN(f'unexpected error checking fsid lock for OSD {osd}: {e}')

    if lock_held:
        if not systemd_status_up:
            WARN(f'systemd claims OSD {osd} is down, but fsid lock is held.')
        return True

    if systemd_status_up:
        WARN(f'systemd claims OSD {osd} is up, but no fsid lock is held (inconsistent state).')
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
        # have to disable the check here, as 'ceph-bluestore-tool show-sharding' will
        # return an error code=1 if the OSD is not sharded, which we want to handle
        # gracefully (return None) instead of raising an exception.
        out, err = run_cmd('ceph-bluestore-tool',
                           ['show-sharding', '--path', osd_path], check=False)
        if ERR_NO_SHARDING in (out, err) or DRYRUN:
            # this is a legacy OSD without RocksDB sharding
            eprint('ceph-bluestore-tool:', FAINT('(no sharding defined)'))
            return None
        return out
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

    try:
        INFO(f'starting reshard of OSD {osd_id} RocksDB...')
        osd_path = osd_libpath(osd_id, cluster)
        out, _ = run_cmd('ceph-bluestore-tool',
                         ['reshard', '--path', osd_path, f'--sharding="{sharding}"'])
        eprint('ceph-bluestore-tool:', FAINT(out))
        INFO(f'reshard process finished for OSD {osd_id}.')
    except RuntimeError as e:
        bailout(f'failed to reshard OSD {osd_id}: {e}')


def fsck(osd_id: int, cluster: str, restart_on_fail: bool) -> List[str]:
    """
    Run a 'ceph-bluestore-tool fsck' on a given OSD device. Exits on failure.

    Args:
        osd_id: The OSD ID to fsck
        cluster: The name of the Ceph cluster
        restart_on_fail: If True, attempt to restart the OSD if fsck fails
    """
    if is_osd_active(osd_id, cluster) and not DRYRUN:
        bailout(f'OSD {osd_id} is active, cannot fsck.')

    try:
        INFO(f'starting fsck of OSD {osd_id}...')
        osd_path = osd_libpath(osd_id, cluster)
        out, _ = run_cmd('ceph-bluestore-tool', ['fsck', '--path', osd_path])
        INFO(f'fsck finished for OSD {osd_id}.')
        return out.splitlines()
    except RuntimeError as e:
        if restart_on_fail:
            WARN(f'fsck failed for OSD {osd_id}, attempting to restart it...')
            try:
                systemctl_op('start', osd=osd_id)
                INFO(f'OSD {osd_id} restarted successfully after fsck failure.')
            except RuntimeError as err:
                bailout(f'failed to restart OSD {osd_id} after fsck failure: {err}')
        bailout(f'failed to fsck OSD {osd_id}: {e}')


def main():
    args = parse_cmdline_args()
    if not (args.yes or args.force):
        WARN(f"this script will reshard OSDs (cluster '{args.cluster}'): {args.osds}")
        eprint('\nRocksDB sharding conf to be applied:\n', FAINT(DEFAULT), '\n')
        eprint(' >>> The operation may take a while and could even cause a loss of OSD(s).')
        eprint(' >>> Each OSD will be stopped, resharded, and restarted in sequence.')
        eprint(" >>> It is recommended to set 'noout' mode during this process.")
        eprint(FAINT(f"     (Use '{os.path.basename(__file__)} --yes' to skip this prompt)\n"))

        WARN('!!! DO NOT RUN THIS SCRIPT ON MULTIPLE OSDs AND/OR HOSTS AT THE SAME TIME !!!\n')
        if input(RED('Are you really sure you wish to proceed? (y/N): ')).strip().lower() != 'y':
            bailout('Aborting.', exit_code=0, prepend=False)

    for i, osd in enumerate(args.osds, start=1):
        eprint(YEL(f'\n[{i}/{len(args.osds)}] Processing OSD {osd} ####################'))

        # wait till Ceph cluster is healthy before starting with each OSD
        if not is_ceph_healthy(args.cluster) and not DRYRUN:
            INFO(f"waiting for Ceph cluster '{args.cluster}' to become healthy...")
            while not is_ceph_healthy(args.cluster):
                sleep(10)

        # stop the OSD service
        start = time()
        if not DRYRUN:
            INFO(f'stopping OSD {osd}...')
            systemctl_op('stop', osd=osd)
        wait_till_inactive(osd, cluster=args.cluster, timeout=args.timeout)
        elapsed = time() - start
        INFO(f'OSD {osd} was shut down in {elapsed:.2f}s')

        # run fsck if requested
        if args.fsck:
            fsck_out = fsck(osd, cluster=args.cluster, restart_on_fail=True)
            if fsck_out:
                eprint('fsck output:', FAINT('\n'.join(fsck_out)))

        # what to do about RocksDB sharding?
        INFO(f'checking RocksDB sharding for OSD {osd}...')
        cur_def = get_sharding_def(osd, cluster=args.cluster)
        if cur_def is None:
            INFO(f'OSD {osd} has no RocksDB sharding -> initialize sharding')
            reshard(osd, cluster=args.cluster, sharding=DEFAULT)
        elif args.force:
            WARN(f'OSD {osd} RocksDB is sharded, but --force is set\n  >>> sharding={cur_def}')
            reshard(osd, cluster=args.cluster, sharding=DEFAULT)
        else:
            INFO(f'OSD {osd} already has RocksDB sharding -> skipping\n  >>> sharding={cur_def}')

        # restart the OSD service
        if not DRYRUN:
            INFO(f'restarting OSD {osd}...')
            systemctl_op('start', osd=osd)
            INFO(f'waiting for OSD {osd} to become active...')
            while not is_osd_active(osd, cluster=args.cluster):
                sleep(1)
            INFO(f'OSD {osd} restarted successfully')

        total = time() - start
        eprint(YEL(f'[{i}/{len(args.osds)}] OSD {osd} reshard finished in {total:.0f}s'))
        if not DRYRUN:
            sleep(10)  # give some time for OSD to stabilize

    INFO('All done.')


if __name__ == "__main__":
    main()
