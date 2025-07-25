#!/usr/bin/env python3

"""
rbd-iostat.py - A script to monitor RBD (RADOS Block Device) I/O statistics.
This script reads I/O statistics from /proc/diskstats for RBD devices and prints them
in a tabular format. It can be run at specified intervals to continuously monitor the I/O
performance of RBD devices.
"""

__author__    = "Mikko Tanner"
__copyright__ = f"(c) {__author__} 2025"
__version__   = "0.2.5-1_20250724"
__license__   = "GPL-3.0-or-later"

import glob
import os
import sys
import termios
import threading
import time
import tty
from argparse import ArgumentParser
from collections import deque
from re import compile as re_compile, error as re_error, Pattern
from typing import Dict, Iterable, List, Optional, Set

PAUSED    = False
QUITTING  = False
MY_NAME   = os.path.basename(__file__)
TERM_ATTR = termios.tcgetattr(sys.stdin.fileno())
INTERVAL  = 0.0
ANSI_RX   = re_compile(r'\x1b\[[0-9;]*m')   # matches standard ANSI color escape sequences
DISKSTATS = '/proc/diskstats'
MAX_COLS  = 20  # maximum number of columns to expect in /proc/diskstats
RBD_GLOB  = '/dev/rbd/*/*'
FIELDS    = {
    # Field definitions for the output table
    # 'ord' is the order of the field in the output (-1 means not displayed),
    # 'x' is the multiplier (scale) for the field value for colorization,
    # 'rev' means if field should be reverse sorted (descending),
    # 'xtra' indicates if the field is an extra (discard/flush) statistic
    'pool':    {'dsc': 'Pool',      'ord': 0,  'x': 1,   'rev': False, 'xtra': False},
    'rbd':     {'dsc': 'RBD image', 'ord': 1,  'x': 1,   'rev': False, 'xtra': False},
    'dev':     {'dsc': 'Device',    'ord': 2,  'x': 1,   'rev': False, 'xtra': False},
    'size':    {'dsc': 'Size',      'ord': 3,  'x': 1,   'rev': True,  'xtra': False},
    'r_io':    {'dsc': 'r_iops',    'ord': 4,  'x': 10,  'rev': True,  'xtra': False},
    'w_io':    {'dsc': 'w_iops',    'ord': 5,  'x': 10,  'rev': True,  'xtra': False},
    'r_mb':    {'dsc': 'rd MB/s',   'ord': 6,  'x': 1,   'rev': True,  'xtra': False},
    'w_mb':    {'dsc': 'wr MB/s',   'ord': 7,  'x': 1,   'rev': True,  'xtra': False},
    'r_rqm':   {'dsc': 'r_rqm/s',   'ord': 8,  'x': 10,  'rev': True,  'xtra': False},
    'r_rqm_p': {'dsc': '%r_rqm',    'ord': 9,  'x': 1,   'rev': True,  'xtra': False},
    'w_rqm':   {'dsc': 'w_rqm/s',   'ord': 10, 'x': 10,  'rev': True,  'xtra': False},
    'w_rqm_p': {'dsc': '%w_rqm',    'ord': 11, 'x': 1,   'rev': True,  'xtra': False},
    'r_wait':  {'dsc': 'r_await',   'ord': 12, 'x': 1,   'rev': True,  'xtra': False},
    'w_wait':  {'dsc': 'w_await',   'ord': 13, 'x': 1,   'rev': True,  'xtra': False},
    'r_sz':    {'dsc': 'rareq-sz',  'ord': 14, 'x': 1,   'rev': True,  'xtra': False},
    'w_sz':    {'dsc': 'wareq-sz',  'ord': 15, 'x': 1,   'rev': True,  'xtra': False},
    'queue':   {'dsc': 'aqu-sz',    'ord': 16, 'x': 1,   'rev': True,  'xtra': False},
    'util':    {'dsc': '%util',     'ord': 17, 'x': 1,   'rev': True,  'xtra': False},
    'd_io':    {'dsc': 'd_iops',    'ord': 18, 'x': 10,  'rev': True,  'xtra': True},
    'd_mb':    {'dsc': 'ds MB/s',   'ord': 19, 'x': 1,   'rev': True,  'xtra': True},
    'd_rqm':   {'dsc': 'd_rqm/s',   'ord': 20, 'x': 10,  'rev': True,  'xtra': True},
    'd_rqm_p': {'dsc': '%d_rqm',    'ord': 21, 'x': 1,   'rev': True,  'xtra': True},
    'd_wait':  {'dsc': 'd_await',   'ord': 22, 'x': 1,   'rev': True,  'xtra': True},
    'd_sz':    {'dsc': 'dareq-sz',  'ord': 23, 'x': 1,   'rev': True,  'xtra': True},
    'f_io':    {'dsc': 'f_iops',    'ord': 24, 'x': 10,  'rev': True,  'xtra': True},
    'f_wait':  {'dsc': 'f_await',   'ord': 25, 'x': 1,   'rev': True,  'xtra': True},
    # virtual sort fields
    'total_io': {'dsc': 'sum_IOs',  'ord': -1, 'x': 10,  'rev': True,  'xtra': False},
    'total_mb': {'dsc': 'sum_MB/s', 'ord': -1, 'x': 1,   'rev': True,  'xtra': False},
    }


class RadosBD:
    """
    Class to represent a locally mapped Rados Block Device (`/dev/rbdX`).

    For ordering, hashing and string representation, the `dev` attribute is used,
    hence for the most purposes this class can be treated as a string of the local
    `/dev/rbdXX` device base name.
    """
    pool: str
    image: str
    dev: str
    """Local device name (e.g., rbd0)."""
    blocks: int
    """Number of 512-byte blocks exposed by the RBD."""
    last_stats: float = 0.0
    """Last timestamp statistics were read for this RBD."""

    def __init__(self, pool: str, image: str, dev: str, blocks: int):
        self.pool = pool
        self.image = image
        self.dev = dev
        self.blocks = blocks

    def __hash__(self):
        return hash(self.dev)

    def __eq__(self, other):
        if isinstance(other, RadosBD):
            return self.dev == other.dev
        if isinstance(other, str):
            return self.dev == other
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, RadosBD):
            return self.dev > other.dev
        if isinstance(other, str):
            return self.dev > other
        return NotImplemented

    def __str__(self):
        return self.dev

    @property
    def path(self) -> str:
        """Path to the local block device."""
        return f'/dev/{self.dev}'

    @property
    def spec(self) -> str:
        """RBD specification in the form of `pool/image`."""
        return f'{self.pool}/{self.image}'

    @property
    def size(self) -> int:
        """Size of the RBD in bytes."""
        return self.blocks * 512

    @property
    def size_str(self) -> str:
        """Human-readable size of the RBD."""
        return humanize_size(self.size)


class DiskStatRow:
    """Represents a single row from `/proc/diskstats`."""
    dev: RadosBD
    """Which device this data belongs to."""
    when: float
    """When the data was sampled (timestamp)."""
    r_io: int
    """Read I/O operations completed successfully."""
    r_merge: int
    """Read I/O operations merged."""
    r_sect: int
    """Read sectors (device-sized blocks)."""
    r_time_ms: int
    """Time spent reading in milliseconds."""
    w_io: int
    """Write I/O operations completed successfully."""
    w_merge: int
    """Write I/O operations merged."""
    w_sect: int
    """Written sectors (device-sized blocks)."""
    w_time_ms: int
    """Time spent writing in milliseconds."""
    io_active: int
    """I/Os currently in progress."""
    io_t_ms: int
    """Time spent doing I/Os in milliseconds."""
    io_t_w_ms: int
    """Weighted time spent doing IO in ms (time is proportional to amount of IOs inflight)."""
    d_io: int = 0
    """Discard requests completed."""
    d_merge: int = 0
    """Discard requests merged."""
    d_sect: int = 0
    """Discarded sectors (device-sized blocks)."""
    d_time_ms: int = 0
    """Time spent discarding in milliseconds."""
    f_io: int = 0
    """Flush requests completed."""
    f_time_ms: int = 0
    """Time spent flushing in milliseconds."""

    def __init__(self, dev: RadosBD, fields: List[str], ts: float):
        """
        Initialize a DiskStatRow instance.

        Args:
            dev: The `RadosBD` this row belongs to.
            fields: A row from `/proc/diskstats`.
            ts: Timestamp when the data was sampled.
        """
        if not dev.dev == fields[2]:
            raise ValueError(f'Device mismatch: {dev.dev} != {fields[2]}')
        self.when = ts
        self.dev = dev
        self.r_io      = int(fields[3])
        self.r_merge   = int(fields[4])
        self.r_sect    = int(fields[5])
        self.r_time_ms = int(fields[6])
        self.w_io      = int(fields[7])
        self.w_merge   = int(fields[8])
        self.w_sect    = int(fields[9])
        self.w_time_ms = int(fields[10])
        self.io_active = int(fields[11])
        self.io_t_ms   = int(fields[12])
        self.io_t_w_ms = int(fields[13])

        # discard fields (if available)
        if len(fields) >= 15:
            self.d_io      = int(fields[14])
            self.d_merge   = int(fields[15])
            self.d_sect    = int(fields[16])
            self.d_time_ms = int(fields[17])

        # flush fields (if available)
        if len(fields) >= 19:
            self.f_io      = int(fields[18])
            self.f_time_ms = int(fields[19])


class DiskStatDelta:
    """The delta between two `DiskStatRow`s. Used to compute the I/O rates between samples."""
    def __init__(self, prev: DiskStatRow, now: DiskStatRow):
        self.delta_t  = now.when      - prev.when
        self.rd_c     = now.r_io      - prev.r_io
        self.wr_c     = now.w_io      - prev.w_io
        self.disc_c   = now.d_io      - prev.d_io
        self.flush_c  = now.f_io      - prev.f_io
        self.rd_m     = now.r_merge   - prev.r_merge
        self.wr_m     = now.w_merge   - prev.w_merge
        self.dsc_m    = now.d_merge   - prev.d_merge
        self.rd_sect  = now.r_sect    - prev.r_sect
        self.wr_sect  = now.w_sect    - prev.w_sect
        self.dsc_sect = now.d_sect    - prev.d_sect
        self.r_time   = now.r_time_ms - prev.r_time_ms
        self.w_time   = now.w_time_ms - prev.w_time_ms
        self.d_time   = now.d_time_ms - prev.d_time_ms
        self.f_time   = now.f_time_ms - prev.f_time_ms
        self.io_t     = now.io_t_ms   - prev.io_t_ms
        self.io_t_w   = now.io_t_w_ms - prev.io_t_w_ms

    def __getitem__(self, item: str) -> float:
        ret = 0.0
        if self.delta_t <= 0:
            return ret  # avoid division by zero

        match item:
            case 'r_io':
                ret = self.rd_c / self.delta_t
            case 'w_io':
                ret = self.wr_c / self.delta_t
            case 'r_mb':
                ret = (self.rd_sect / 2048) / self.delta_t
            case 'w_mb':
                ret = (self.wr_sect / 2048) / self.delta_t
            case 'r_rqm':
                ret = self.rd_m / self.delta_t
            case 'w_rqm':
                ret = self.wr_m / self.delta_t
            case 'r_rqm_p':
                if self.rd_m + self.rd_c > 0:
                    ret = 100 * self.rd_m / (self.rd_m + self.rd_c)
            case 'w_rqm_p':
                if self.wr_m + self.wr_c > 0:
                    ret = 100 * self.wr_m / (self.wr_m + self.wr_c)
            case 'r_wait':
                if self.rd_c > 0:
                    ret = self.r_time / self.rd_c
            case 'w_wait':
                if self.wr_c > 0:
                    ret = self.w_time / self.wr_c
            case 'r_sz':
                if self.rd_c > 0:
                    ret = self.rd_sect / self.rd_c
            case 'w_sz':
                if self.wr_c > 0:
                    ret = self.wr_sect / self.wr_c
            case 'queue':
                ret = self.io_t_w / (self.delta_t * 1e3)
            case 'util':
                ret = 100 * self.io_t / (self.delta_t * 1e3)
            case 'd_io':
                ret = self.disc_c / self.delta_t
            case 'd_mb':
                ret = (self.dsc_sect / 2048) / self.delta_t
            case 'd_rqm':
                ret = self.dsc_m / self.delta_t
            case 'd_rqm_p':
                if self.dsc_m + self.disc_c > 0:
                    ret = 100 * self.dsc_m / (self.dsc_m + self.disc_c)
            case 'd_wait':
                if self.disc_c > 0:
                    ret = self.d_time / self.disc_c
            case 'd_sz':
                if self.disc_c > 0:
                    ret = self.dsc_sect / self.disc_c
            case 'f_io':
                ret = self.flush_c / self.delta_t
            case 'f_wait':
                if self.flush_c > 0:
                    ret = self.f_time / self.flush_c
            case 'total_io':    # total IOPS (read + write + discard + flush)
                ret = (self.rd_c + self.wr_c + self.disc_c + self.flush_c) / self.delta_t
            case 'total_mb':    # total I/O in MB/s (read + write + discard)
                ret = ((self.rd_sect + self.wr_sect + self.dsc_sect) / 2048) / self.delta_t
            case _:
                raise KeyError(f'Unknown field: {item}')

        return ret


def parse_cmdline_args():
    """Parse command-line arguments."""
    args = ArgumentParser(description='RBD I/O statistics monitor')
    args.add_argument('inter', nargs='?', type=float, default=INTERVAL,
                      help='Continuous statistics interval (default: 0, i.e., one-shot)')
    args.add_argument('--pool', '-P', help='Which pool to monitor (default: all)')
    args.add_argument('--name', '-N', help='Which RBDs to monitor [regex] (default: all)')
    args.add_argument('--hist', '-H', type=int, default=1,
                      help='Statistics history entries to keep (default: 1, min: 1)')
    args.add_argument('--sort', choices=tuple(f[0] for f in FIELDS.items() if not f[1]['xtra']),
                      help='Which column to sort by (default: RBD name / pool name)')
    args.add_argument('--extra', '-x', action='store_true',
                      help='Include extended (discard/flush) statistics in the output')
    args.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    p = args.parse_args()

    # validate/adjust arguments
    p.inter = max(0.0, p.inter) # ensure interval is non-negative
    p.hist = max(1, p.hist)     # ensure history is at least 1 entry

    # RBD pool and name filtering
    if p.pool:
        global RBD_GLOB
        p.pool = p.pool.strip().lower()     # normalize pool name
        RBD_GLOB = RBD_GLOB.replace('*', p.pool, 1)
    if p.name:
        try:
            p.name = re_compile(p.name)  # validate regex
        except re_error as e:
            args.error(f'Invalid regex for RBD name: {e}')

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

def BLU(_s: str):
    """Make a string bold blue."""
    return f'{_CC(34, 1)}{_s}{_CC(0)}'

def MAG(_s: str):
    """Make a string bold magenta."""
    return f'{_CC(35, 1)}{_s}{_CC(0)}'

def BOLD(_s: str):
    """Make a string bold."""
    return f'{_CC(1)}{_s}{_CC(0)}'

def FAINT(_s: str):
    """Make a string faint (dimmed)."""
    return f'{_CC(2)}{_s}{_CC(0)}'


def eprint(*values, **kwargs):
    """Mimic print() but write to stderr."""
    print(*values, file=sys.stderr, **kwargs)


def read_oneline_file(f: str):
    """Read a single line from a file, typically used for files under /sys etc."""
    return open(f, encoding='utf-8').readline()


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


def humanize_size(size: int) -> str:
    """Convert a size in bytes to a human-readable format."""
    base = 1024
    if size < base:
        return f'{size} B'
    if size < base**2:
        return f'{size / base:.0f} KiB'
    if size < base**3:
        return f'{size / base**2:.0f} MiB'
    if size < base**4:
        return f'{size / base**3:.0f} GiB'

    return f'{size / base**4:.1f} TiB'


def clear_terminal():
    """Clear the terminal screen if stdout is a terminal."""
    if sys.stdout.isatty():
        os.system('clear' if os.name == 'posix' else 'cls')


def restore_terminal():
    """Restore terminal settings to the original state."""
    termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, TERM_ATTR)


def getch():
    """Read a single character from stdin without echoing it."""
    try:
        tty.setcbreak(sys.stdin.fileno())
        ch = sys.stdin.read(1)
    except KeyboardInterrupt:
        ch = '\x03'  # handle Ctrl+C
    finally:
        restore_terminal()
    return ch


def key_event_handler():
    """Listen for key presses to control program flow."""
    global PAUSED, QUITTING, INTERVAL
    int_step = 0.5  # interval adjustment step
    while True:
        ch = getch()
        match ch:
            case ' ':
                PAUSED = not PAUSED
                eprint('\n' + 'Paused - press <space> to resume.' if PAUSED else 'Resuming...')
            case 'q':
                eprint('\nExiting...')
                QUITTING = True
                break
            case '+':
                if INTERVAL <= 9.5:  # limit max interval to 10 seconds
                    INTERVAL += int_step
                    eprint(f'\nInterval increased to {INTERVAL:.1f} seconds.')
            case '-':
                if INTERVAL >= 1:   # limit min interval to 1 seconds
                    INTERVAL -= int_step
                    eprint(f'\nInterval decreased to {INTERVAL:.1f} seconds.')
            case 'h':
                PAUSED = True
                eprint(f"\n{MY_NAME} - Press 'q' to exit, '<space>' to pause/resume,",
                       f"'+' / '-' to incr/decr display interval by {int_step} s.")
            case '\x03':    # handle Ctrl+C gracefully, otherwise terminal may be garbled
                eprint('\nInterrupted (listener thread)...')
                restore_terminal()
                QUITTING = True
                break
            case _:         # ignore other keys
                continue


def build_mapping(patt: Optional[Pattern]):
    """Build mapping from `rbdX` to `pool/rbd_name`"""
    mapping: Dict[str, RadosBD] = {}
    skipped: Set[str] = set()
    for link in glob.glob(RBD_GLOB):
        if os.path.islink(link):
            if '-part' in link:  # skip partition links
                continue

            tgt = os.readlink(link)
            if tgt.startswith('../../rbd'):
                dev = tgt[6:]
                pool = os.path.basename(os.path.dirname(link))
                rbd_name = os.path.basename(link)

                # are we filtering by RBD name?
                if patt and not patt.match(rbd_name):
                    skipped.add(dev)
                    continue

                blocks = int(read_oneline_file(f'/sys/block/{dev}/size').strip())
                mapping[dev] = RadosBD(pool, image=rbd_name, dev=dev, blocks=blocks)

    return mapping, skipped


def read_stats(mapping: Dict[str, RadosBD], skipped: Set[str]):
    """
    Read I/O statistics from `/proc/diskstats` for RBD devices.

    ord | Field
    --- | -----
    0   | major number
    1   | minor number
    2   | device name (e.g., rbd0)
    3   | reads completed successfully
    4   | reads merged
    5   | read sectors
    6   | time spent reading (ms)
    7   | writes completed
    8   | writes merged
    9   | written sectors
    10  | time spent writing (ms)
    11  | I/Os currently in progress
    12  | time spent doing I/Os (ms)
    13  | weighted time spent doing I/Os (ms)
    14  | discard requests completed (if available)
    15  | discard requests merged (if available)
    16  | iscarded sectors (if available)
    17  | time spent discarding (if available)
    18  | flush requests completed (if available)
    19  | time spent flushing (if available)
    """
    stats: Dict[str, DiskStatRow] = {}
    with open(DISKSTATS, encoding='utf-8') as f:
        timestamp = time.time()
        for line in f:
            parts = line.split()
            num_fields = len(parts)
            dev = parts[2]

            if dev in skipped or not dev.startswith('rbd') or dev not in mapping:
                continue

            if num_fields > MAX_COLS or num_fields < MAX_COLS - 6:
                eprint(f'WARN: unsupported number of fields in {DISKSTATS} for {dev}:',
                       f'expected {MAX_COLS-6} to {MAX_COLS}, got {num_fields}')
                continue

            # pad with zeroes for missing fields to avoid IndexError later
            if num_fields < 20:
                parts.extend(['0'] * (MAX_COLS - num_fields))

            # construct DiskStatRow object and associate it with the RadosBD
            mapping[dev].last_stats = timestamp
            stats[dev] = DiskStatRow(mapping[dev], fields=parts, ts=timestamp)
    return stats


def sort_stats(stats: List[List[str]], key: str | None, values: List[float]):
    """Sort the statistics data in-place based on the specified column."""
    match key:
        case None | 'rbd':
            stats.sort(key=lambda x: (x[1], x[0]))
        case 'pool':
            stats.sort(key=lambda x: (x[0], x[1]))
        case 'dev':
            stats.sort(key=lambda x: x[2])
        case _:
            # sort based on values[i] as primary, indexed[i][1] as secondary (rbd image name)
            # this extra indexing step is needed for matching each row with its sort value
            indexed = list(enumerate(stats))
            indexed.sort(key=lambda x: (values[x[0]], x[1][1]), reverse=FIELDS[key]['rev'])

            # unpack the sorted 'stats' back in-place
            for i, (_, row) in enumerate(indexed):
                stats[i] = row


def colorize(text: str, val: float | int, scale = 1.0):
    """Colorize the text based on the value."""
    if val == 0:
        return FAINT(text)
    if val < 0.5 * scale:
        return text
    if val < 25 * scale:
        return BOLD(text)
    if val < 50 * scale:
        return BLU(text)
    if val < 70 * scale:
        return MAG(text)
    if val < 85 * scale:
        return YEL(text)
    return RED(text)


def parse_data(mapping: Dict[str, RadosBD], prev: Dict[str, DiskStatRow],
               now: Dict[str, DiskStatRow], xtra: bool,
               sort_key: Optional[str] = None):
    """
    Parse the current statistics data to compute I/O rates. Also returns a list of
    raw values for selected sorting column, if required.
    """
    def colorize_row(field: str, precision: int) -> str:
        """Helper for colorizing a row based on the field and its value."""
        val = delta[field]
        return colorize(f'{val:.{precision}f}', val=val, scale=FIELDS[field]['x'])

    data: List[str] = []
    shadow: List[float] = []
    for dev in sorted(mapping.keys()):
        if dev not in now:
            continue

        delta = DiskStatDelta(prev=prev[dev], now=now[dev])
        row = [
            mapping[dev].pool,          # Pool name
            GRN(mapping[dev].image),    # RBD name
            mapping[dev].dev,           # /dev/rbdX
            mapping[dev].size_str,      # human-readable size
            colorize_row('r_io', 0),
            colorize_row('w_io', 0),
            colorize_row('r_mb', 1),
            colorize_row('w_mb', 1),
            colorize_row('r_rqm', 0),
            f"{delta['r_rqm_p']:.2f}",
            colorize_row('w_rqm', 0),
            f"{delta['w_rqm_p']:.2f}",
            colorize_row('r_wait', 1),
            colorize_row('w_wait', 1),
            f"{delta['r_sz']:.1f}",
            f"{delta['w_sz']:.1f}",
            colorize_row('queue', 1),
            colorize_row('util', 2),
            ]

        if xtra:    # add discard+flush statistics if requested
            row.extend([
                f"{delta['d_io']:.0f}",
                f"{delta['d_mb']:.1f}",
                f"{delta['d_rqm']:.1f}",
                f"{delta['d_rqm_p']:.2f}",
                f"{delta['d_wait']:.1f}",
                f"{delta['d_sz']:.1f}",
                f"{delta['f_io']:.0f}",
                f"{delta['f_wait']:.2f}",
                ])

        data.append(row)
        if sort_key:
            # if sorting is requested, we want to keep the actual values of that column around
            # virtual sort keys also need special handling, as they have no column in the output
            match sort_key:
                case 'rbd' | 'pool' | 'dev':    # strings require no shadowing
                    pass
                case 'size':
                    shadow.append(mapping[dev].size)
                case 'total_io':
                    shadow.append(delta['r_io'] + delta['w_io'] + delta['d_io'] + delta['f_io'])
                case 'total_mb':
                    shadow.append(delta['r_mb'] + delta['w_mb'] + delta['d_mb'])
                case _:
                    shadow.append(delta[sort_key])

    return data, shadow


def main():
    args       = parse_cmdline_args()
    continuous = args.inter > 0
    rbd_map, skip = build_mapping(patt=args.name)
    if not rbd_map:
        print('No mapped Rados Block Devices found.')
        sys.exit(0)

    if continuous:
        global INTERVAL
        INTERVAL = args.inter
        listener_t = threading.Thread(target=key_event_handler, daemon=True)
        listener_t.start()

    history: deque[Dict[str, DiskStatRow]] = deque(maxlen=args.hist)
    if args.extra:
        headers = tuple(f['dsc'] for f in FIELDS.values() if f['ord'] >= 0)
    else:
        headers = tuple(f['dsc'] for f in FIELDS.values() if f['ord'] >= 0 and not f['xtra'])

    while True:
        stats = read_stats(rbd_map, skipped=skip)

        if len(history) == 0:
            # assume first read is at system boot
            prev_time  = time.time() - float(read_oneline_file('/proc/uptime').split()[0])
            prev_stats = {dev: DiskStatRow(dev=rbd_map[dev],
                                           fields=['0', '0', dev] + ['0'] * MAX_COLS,
                                           ts=prev_time)
                          for dev in stats}
            if continuous:
                history.append(prev_stats)
        else:
            prev_stats = history[-1]

        # parse & display the data
        if not PAUSED:
            data, shadow = parse_data(rbd_map, prev=prev_stats, now=stats,
                                      xtra=args.extra, sort_key=args.sort)
            if continuous:
                clear_terminal()

            sort_stats(data, key=args.sort, values=shadow)
            print(simple_tabulate(data, headers=tuple(BOLD(h) for h in headers)))

        # exit here if we are not in continuous mode or if quitting
        if not continuous or QUITTING:
            break

        # keep history
        history.append(stats)

        # Sleep for the specified interval in continuous mode,
        # but also be responsive to user quit requests.
        slept = 0.0
        delay = min(0.2, INTERVAL)
        while slept < INTERVAL and not QUITTING:
            time.sleep(delay)
            slept += delay
            if QUITTING:
                break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        eprint('\nInterrupted...')
        sys.exit(0)
    finally:
        # make sure to restore terminal settings
        restore_terminal()
