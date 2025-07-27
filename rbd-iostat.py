#!/usr/bin/env python3

"""
rbd-iostat.py - A script to monitor RBD (RADOS Block Device) I/O statistics.
This script reads I/O statistics from /proc/diskstats for RBD devices and prints them
in a tabular format. It can be run at specified intervals to continuously monitor the I/O
performance of RBD devices.
"""

__author__    = "Mikko Tanner"
__copyright__ = f"(c) {__author__} 2025"
__version__   = "0.2.7-1_20250727"
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
from enum import Enum, IntEnum
from re import compile as re_compile, error as re_error, Pattern
from typing import Dict, Iterable, List, Optional, Set

PAUSED    = False
QUITTING  = False
MY_NAME   = os.path.basename(__file__)
TERM_ATTR = termios.tcgetattr(sys.stdin.fileno())
INTERVAL  = 0.0
ANSI_RX   = re_compile(r'\x1b\[[0-9;]*m')   # matches standard ANSI color escape sequences
DISKSTATS = '/proc/diskstats'
RBD_GLOB  = '/dev/rbd/*/*'

class FieldAttrs:
    __slots__ = ('header', 'pos', 'x', 'rev', 'xtra', 'virt')

    def __init__(self, header: str, pos: int, x = 1, rev = True, xtra = False, virt = False):
        self.header = header
        """Column header (description) for display purposes"""
        self.pos = pos
        """Column position in the output table"""
        self.x = x
        """The multiplier (scale) of the field value for colorization"""
        self.rev = rev
        """Whether the field should be reverse sorted (descending)"""
        self.xtra = xtra
        """Indicates if the field is an extra (discard/flush) statistic"""
        self.virt = virt
        """Indicates if the field is a virtual (aggregate) sort field (not displayed)"""


class OutputField(Enum):
    """Output fields and their order in the table."""
    POOL    = FieldAttrs('Pool',      pos=0,    rev=False)
    RBD     = FieldAttrs('RBD image', pos=1,    rev=False)
    DEV     = FieldAttrs('Device',    pos=2,    rev=False)
    SIZE    = FieldAttrs('Size',      pos=3)
    R_IOPS  = FieldAttrs('r_iops',    pos=4,    x=10)
    W_IOPS  = FieldAttrs('w_iops',    pos=5,    x=10)
    R_MBPS  = FieldAttrs('rd MB/s',   pos=6)
    W_MBPS  = FieldAttrs('wr MB/s',   pos=7)
    R_RQM   = FieldAttrs('r_rqm/s',   pos=8,    x=10)
    R_RQM_P = FieldAttrs('%r_rqm',    pos=9)
    W_RQM   = FieldAttrs('w_rqm/s',   pos=10)
    W_RQM_P = FieldAttrs('%w_rqm',    pos=11)
    R_WAIT  = FieldAttrs('r_await',   pos=12)
    W_WAIT  = FieldAttrs('w_await',   pos=13)
    R_SIZE  = FieldAttrs('rareq-sz',  pos=14,   x=20)
    W_SIZE  = FieldAttrs('wareq-sz',  pos=15,   x=20)
    QUEUE   = FieldAttrs('aqu-sz',    pos=16)
    UTIL    = FieldAttrs('%util',     pos=17)
    D_IOPS  = FieldAttrs('d_iops',    pos=18,   x=10, xtra=True)
    D_MBPS  = FieldAttrs('ds MB/s',   pos=19,         xtra=True)
    D_RQM   = FieldAttrs('d_rqm/s',   pos=20,   x=10, xtra=True)
    D_RQM_P = FieldAttrs('%d_rqm',    pos=21,         xtra=True)
    D_WAIT  = FieldAttrs('d_await',   pos=22,         xtra=True)
    D_SIZE  = FieldAttrs('dareq-sz',  pos=23,         xtra=True)
    F_IOPS  = FieldAttrs('f_iops',    pos=24,   x=10, xtra=True)
    F_WAIT  = FieldAttrs('f_await',   pos=25,         xtra=True)
    SUM_IO  = FieldAttrs('sum_IOs',   pos=-1,   x=10, virt=True)
    """Total IOPS (read + write + discard + flush)"""
    SUM_MB  = FieldAttrs('sum_MB/s',  pos=-1,         virt=True)
    """Total I/O in MB/s (read + write + discard)"""


class DiskStatField(IntEnum):
    """Fields in /proc/diskstats."""
    MAJOR      = 0
    MINOR      = 1
    DEV        = 2
    """Device name (e.g., rbd0)."""
    R_IO_OPS   = 3
    """Read I/O operations completed successfully."""
    R_MERGES   = 4
    """Read I/O operations merged."""
    R_SECTORS  = 5
    """Read sectors (device-sized blocks)."""
    R_TIME_MS  = 6
    """Time spent reading in milliseconds."""
    W_IO_OPS   = 7
    """Write I/O operations completed successfully."""
    W_MERGES   = 8
    """Write I/O operations merged."""
    W_SECTORS  = 9
    """Written sectors (device-sized blocks)."""
    W_TIME_MS  = 10
    """Time spent writing in milliseconds."""
    IO_ACTIVE  = 11
    """I/Os currently in progress."""
    IO_TIME_MS = 12
    """Time spent doing I/Os in milliseconds."""
    IO_TM_W_MS = 13
    """Weighted time spent doing IO in ms (time is proportional to amount of IOs inflight)."""
    D_IO_OPS   = 14
    """Discard requests completed."""
    D_MERGES   = 15
    """Discard requests merged."""
    D_SECTORS  = 16
    """Discarded sectors (device-sized blocks)."""
    D_TIME_MS  = 17
    """Time spent discarding in milliseconds."""
    F_IO_OPS   = 18
    """Flush requests completed."""
    F_TIME_MS  = 19
    """Time spent flushing in milliseconds."""


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
    r_merge: int
    r_sect: int
    r_time_ms: int
    w_io: int
    w_merge: int
    w_sect: int
    w_time_ms: int
    io_active: int
    io_t_ms: int
    io_t_w_ms: int
    d_io: int = 0
    d_merge: int = 0
    d_sect: int = 0
    d_time_ms: int = 0
    f_io: int = 0
    f_time_ms: int = 0

    def __init__(self, dev: RadosBD, fields: List[str], ts: float):
        """
        Initialize a DiskStatRow instance.

        Args:
            dev: The `RadosBD` this row belongs to.
            fields: A row from `/proc/diskstats`.
            ts: Timestamp when the data was sampled.
        """
        if not dev.dev == fields[DiskStatField.DEV]:
            raise ValueError(f'Device mismatch: {dev.dev} != {fields[DiskStatField.DEV]}')
        self.when = ts
        self.dev = dev
        # convert numeric fields to integers
        fields[DiskStatField.R_IO_OPS:] = map(int, fields[DiskStatField.R_IO_OPS:])
        self.r_io      = fields[DiskStatField.R_IO_OPS]
        self.r_merge   = fields[DiskStatField.R_MERGES]
        self.r_sect    = fields[DiskStatField.R_SECTORS]
        self.r_time_ms = fields[DiskStatField.R_TIME_MS]
        self.w_io      = fields[DiskStatField.W_IO_OPS]
        self.w_merge   = fields[DiskStatField.W_MERGES]
        self.w_sect    = fields[DiskStatField.W_SECTORS]
        self.w_time_ms = fields[DiskStatField.W_TIME_MS]
        self.io_active = fields[DiskStatField.IO_ACTIVE]
        self.io_t_ms   = fields[DiskStatField.IO_TIME_MS]
        self.io_t_w_ms = fields[DiskStatField.IO_TM_W_MS]

        # discard fields (if available)
        if len(fields) >= 15:
            self.d_io      = fields[DiskStatField.D_IO_OPS]
            self.d_merge   = fields[DiskStatField.D_MERGES]
            self.d_sect    = fields[DiskStatField.D_SECTORS]
            self.d_time_ms = fields[DiskStatField.D_TIME_MS]

        # flush fields (if available)
        if len(fields) >= 19:
            self.f_io      = fields[DiskStatField.F_IO_OPS]
            self.f_time_ms = fields[DiskStatField.F_TIME_MS]


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

    def __getitem__(self, item: OutputField) -> float:
        if not isinstance(item, OutputField):
            raise TypeError(f'Expected OutputField, got {type(item)}')

        ret = 0.0
        if self.delta_t <= 0:
            return ret  # avoid division by zero

        match item:
            case OutputField.R_IOPS:
                ret = self.rd_c / self.delta_t
            case OutputField.W_IOPS:
                ret = self.wr_c / self.delta_t
            case OutputField.R_MBPS:
                ret = (self.rd_sect / 2048) / self.delta_t
            case OutputField.W_MBPS:
                ret = (self.wr_sect / 2048) / self.delta_t
            case OutputField.R_RQM:
                ret = self.rd_m / self.delta_t
            case OutputField.W_RQM:
                ret = self.wr_m / self.delta_t
            case OutputField.R_RQM_P:
                if self.rd_m + self.rd_c > 0:
                    ret = 100 * self.rd_m / (self.rd_m + self.rd_c)
            case OutputField.W_RQM_P:
                if self.wr_m + self.wr_c > 0:
                    ret = 100 * self.wr_m / (self.wr_m + self.wr_c)
            case OutputField.R_WAIT:
                if self.rd_c > 0:
                    ret = self.r_time / self.rd_c
            case OutputField.W_WAIT:
                if self.wr_c > 0:
                    ret = self.w_time / self.wr_c
            case OutputField.R_SIZE:
                if self.rd_c > 0:
                    ret = self.rd_sect / self.rd_c
            case OutputField.W_SIZE:
                if self.wr_c > 0:
                    ret = self.wr_sect / self.wr_c
            case OutputField.QUEUE:
                ret = self.io_t_w / (self.delta_t * 1e3)
            case OutputField.UTIL:
                ret = 100 * self.io_t / (self.delta_t * 1e3)
            case OutputField.D_IOPS:
                ret = self.disc_c / self.delta_t
            case OutputField.D_MBPS:
                ret = (self.dsc_sect / 2048) / self.delta_t
            case OutputField.D_RQM:
                ret = self.dsc_m / self.delta_t
            case OutputField.D_RQM_P:
                if self.dsc_m + self.disc_c > 0:
                    ret = 100 * self.dsc_m / (self.dsc_m + self.disc_c)
            case OutputField.D_WAIT:
                if self.disc_c > 0:
                    ret = self.d_time / self.disc_c
            case OutputField.D_SIZE:
                if self.disc_c > 0:
                    ret = self.dsc_sect / self.disc_c
            case OutputField.F_IOPS:
                ret = self.flush_c / self.delta_t
            case OutputField.F_WAIT:
                if self.flush_c > 0:
                    ret = self.f_time / self.flush_c
            case OutputField.SUM_IO:
                ret = (self.rd_c + self.wr_c + self.disc_c + self.flush_c) / self.delta_t
            case OutputField.SUM_MB:
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
    args.add_argument('--sort', choices=[f.name.lower() for f in OutputField if not f.value.xtra],
                      help='Which column to sort by (default: RBD name / pool name)')
    args.add_argument('--extra', '-x', action='store_true', dest='xtra',
                      help='Include extended (discard/flush) statistics in the output')
    args.add_argument('--totals', '-T', action='store_true', dest='aggr',
                      help='Include combined statistics (IOPS, MB/s etc) in the output')
    args.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    p = args.parse_args()

    # validate/adjust arguments
    p.inter = max(0.0, p.inter) # ensure interval is non-negative
    p.hist = max(1, p.hist)     # ensure history is at least 1 entry
    p.sort = OutputField[p.sort.upper()] if p.sort else None

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
    max_cols = len(DiskStatField)
    with open(DISKSTATS, encoding='utf-8') as f:
        timestamp = time.time()
        for line in f:
            parts = line.split()
            num_fields = len(parts)
            dev = parts[DiskStatField.DEV]

            if dev in skipped or not dev.startswith('rbd') or dev not in mapping:
                continue

            if num_fields > max_cols or num_fields < max_cols - 6:
                eprint(f'WARN: unsupported number of fields in {DISKSTATS} for {dev}:',
                       f'expected {max_cols-6} to {max_cols}, got {num_fields}')
                continue

            # pad with zeroes for missing fields to avoid IndexError later
            if num_fields < max_cols:
                parts.extend(['0'] * (max_cols - num_fields))

            # construct DiskStatRow object and associate it with the RadosBD
            mapping[dev].last_stats = timestamp
            stats[dev] = DiskStatRow(mapping[dev], fields=parts, ts=timestamp)
    return stats


def sort_stats(stats: List[List[str]], key: Optional[OutputField], values: List[float]):
    """Sort the statistics data in-place based on the specified column."""
    match key:
        case None | OutputField.RBD:
            stats.sort(key=lambda x: (x[OutputField.RBD.value.pos], x[OutputField.POOL.value.pos]))
        case OutputField.POOL:
            stats.sort(key=lambda x: (x[OutputField.POOL.value.pos], x[OutputField.RBD.value.pos]))
        case OutputField.DEV:
            stats.sort(key=lambda x: x[OutputField.DEV.value.pos])
        case _:
            # sort based on values[i] as primary, indexed[i][1] as secondary (rbd image name)
            # this extra indexing step is needed for matching each row with its sort value
            indexed = list(enumerate(stats))
            indexed.sort(key=lambda x: (values[x[0]], x[1][OutputField.RBD.value.pos]),
                         reverse=key.value.rev)

            # unpack the sorted 'stats' back in-place
            for i, (_, row) in enumerate(indexed):
                stats[i] = row


def colorize(text: str, val: float | int, scale = 1.0):
    """Colorize the text based on the value."""
    if val == 0:
        return '-'
    if val < 1.5 * scale:
        return FAINT(text)
    if val < 25 * scale:
        return text
    if val < 50 * scale:
        return BLU(text)
    if val < 70 * scale:
        return MAG(text)
    if val < 85 * scale:
        return YEL(text)
    return RED(text)


def parse_data(mapping: Dict[str, RadosBD], prev: Dict[str, DiskStatRow],
               now: Dict[str, DiskStatRow], xtra: bool, aggr: bool, key: Optional[OutputField]):
    """
    Parse the current statistics data to compute I/O rates. Also returns a list of
    raw values for selected sorting column, if required. Aggregates certain fields
    into totals if requested as well.
    """
    def fmt_cell(field: OutputField, precision: int) -> str:
        """Helper for colorizing a cell based on the field and its value."""
        val = delta[field]
        return colorize(f'{val:.{precision}f}', val, scale=field.value.x)

    data: List[str] = []
    shadow: List[float] = []
    totals: List[Optional[str]] = None
    tmp = None
    if aggr:
        tmp = {'r_io': 0.0, 'w_io': 0.0, 'r_mb': 0.0, 'w_mb': 0.0, 'r_rqm': 0.0, 'w_rqm': 0.0,
               'd_io': 0.0, 'd_mb': 0.0, 'd_rqm': 0.0, 'f_io': 0.0, 'size': 0,
               'queue': 0.0, 'util': 0.0, 'n_queue': 0, 'n_util': 0}

    for dev in sorted(mapping.keys()):
        if dev not in now:
            continue

        delta = DiskStatDelta(prev=prev[dev], now=now[dev])
        row = [
            mapping[dev].pool,          # Pool name
            GRN(mapping[dev].image),    # RBD name
            mapping[dev].dev,           # /dev/rbdX
            mapping[dev].size_str,      # human-readable size
            fmt_cell(OutputField.R_IOPS, 0), fmt_cell(OutputField.W_IOPS, 0),
            fmt_cell(OutputField.R_MBPS, 1), fmt_cell(OutputField.W_MBPS, 1),
            fmt_cell(OutputField.R_RQM, 0),  fmt_cell(OutputField.R_RQM_P, 2),
            fmt_cell(OutputField.W_RQM, 0),  fmt_cell(OutputField.W_RQM_P, 2),
            fmt_cell(OutputField.R_WAIT, 1), fmt_cell(OutputField.W_WAIT, 1),
            fmt_cell(OutputField.R_SIZE, 1), fmt_cell(OutputField.W_SIZE, 1),
            fmt_cell(OutputField.QUEUE, 1),  fmt_cell(OutputField.UTIL, 2),
            ]

        if aggr:  # add current RBD to the aggregate totals
            tmp['size']  += mapping[dev].size
            tmp['r_io']  += delta[OutputField.R_IOPS]
            tmp['w_io']  += delta[OutputField.W_IOPS]
            tmp['r_mb']  += delta[OutputField.R_MBPS]
            tmp['w_mb']  += delta[OutputField.W_MBPS]
            tmp['r_rqm'] += delta[OutputField.R_RQM]
            tmp['w_rqm'] += delta[OutputField.W_RQM]
            # weighted average aqu-sz and %util
            if delta[OutputField.QUEUE] > 0:
                tmp['n_queue'] += 1
                tmp['queue'] += delta[OutputField.QUEUE]
            if delta[OutputField.UTIL] > 0:
                tmp['n_util'] += 1
                tmp['util']  += delta[OutputField.UTIL]

        if xtra:    # add discard+flush statistics if requested
            row.extend([
                f"{delta[OutputField.D_IOPS]:.0f}",
                f"{delta[OutputField.D_MBPS]:.1f}",
                f"{delta[OutputField.D_RQM]:.1f}",
                f"{delta[OutputField.D_RQM_P]:.2f}",
                f"{delta[OutputField.D_WAIT]:.1f}",
                f"{delta[OutputField.D_SIZE]:.1f}",
                f"{delta[OutputField.F_IOPS]:.0f}",
                f"{delta[OutputField.F_WAIT]:.2f}",
                ])

            if aggr:
                tmp['d_io']  += delta[OutputField.D_IOPS]
                tmp['d_mb']  += delta[OutputField.D_MBPS]
                tmp['d_rqm'] += delta[OutputField.D_RQM]
                tmp['f_io']  += delta[OutputField.F_IOPS]

        data.append(row)
        if key:
            # if sorting is requested, we want to keep the actual values of that column around
            # virtual sort keys also need special handling, as they have no column in the output
            match key:
                case OutputField.RBD | OutputField.POOL | OutputField.DEV:
                    pass    # strings require no shadowing
                case OutputField.SIZE:
                    shadow.append(mapping[dev].size)
                case OutputField.SUM_IO:
                    shadow.append(delta[OutputField.R_IOPS] + delta[OutputField.W_IOPS] +
                                  delta[OutputField.D_IOPS] + delta[OutputField.F_IOPS])
                case OutputField.SUM_MB:
                    shadow.append(delta[OutputField.R_MBPS] + delta[OutputField.W_MBPS] +
                                  delta[OutputField.D_MBPS])
                case _:
                    shadow.append(delta[key])

        if aggr:
            # avoid division by zero with aqu-sz and %util
            n_qu, n_ut  = max(1, tmp['n_queue']), max(1, tmp['n_util'])
            totals = [None, 'ALL RBDs', None, humanize_size(tmp['size']),
                      f"{tmp['r_io']:.0f}", f"{tmp['w_io']:.0f}",
                      f"{tmp['r_mb']:.1f}", f"{tmp['w_mb']:.1f}",
                      f"{tmp['r_rqm']:.0f}", None, f"{tmp['w_rqm']:.0f}",
                      None, None, None, None, None,
                      f"{tmp['queue']/n_qu:.1f}", f"{tmp['util']/n_ut:.2f}"]
            if xtra:
                totals.extend([f"{tmp['d_io']:.0f}", f"{tmp['d_mb']:.1f}",
                               f"{tmp['d_rqm']:.0f}", None, None, None,
                               f"{tmp['f_io']:.0f}", None])

    return data, shadow, totals


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
    if args.xtra:
        headers = [f.value.header for f in OutputField if not f.value.virt]
    else:
        headers = [f.value.header for f in OutputField if not (f.value.xtra or f.value.virt)]

    while True:
        stats = read_stats(rbd_map, skipped=skip)

        if len(history) == 0:
            # assume first read is at system boot
            prev_time  = time.time() - float(read_oneline_file('/proc/uptime').split()[0])
            prev_stats = {dev: DiskStatRow(dev=rbd_map[dev],
                                           fields=['0', '0', dev] + ['0'] * len(DiskStatField),
                                           ts=prev_time)
                          for dev in stats}
            if continuous:
                history.append(prev_stats)
        else:
            prev_stats = history[-1]

        # parse & display the data
        if not PAUSED:
            data, shadow, aggr = parse_data(rbd_map, prev=prev_stats, now=stats,
                                            xtra=args.xtra, aggr=args.aggr, key=args.sort)
            if continuous:
                clear_terminal()

            sort_stats(data, key=args.sort, values=shadow)
            # add totals row if requested as the first row after sorting
            if args.aggr:
                data.insert(0, aggr)
            print(simple_tabulate(data, headers=headers))

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
