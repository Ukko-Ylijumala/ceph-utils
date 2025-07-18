#!/usr/bin/env python3

"""
rbd-iostat.py - A script to monitor RBD (RADOS Block Device) I/O statistics.
This script reads I/O statistics from /proc/diskstats for RBD devices and prints them
in a tabular format. It can be run at specified intervals to continuously monitor the I/O
performance of RBD devices.
"""

__author__    = "Mikko Tanner"
__copyright__ = f"(c) {__author__} 2025"
__version__   = "0.2.3-1_20250718"
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
from typing import Dict, Iterable, List, Optional

PAUSED    = False
QUITTING  = False
MY_NAME   = os.path.basename(__file__)
TERM_ATTR = termios.tcgetattr(sys.stdin.fileno())
INTERVAL  = 0.0
ANSI_RX   = re_compile(r'\x1b\[[0-9;]*m')   # matches standard ANSI color escape sequences
DISKSTATS = '/proc/diskstats'
RBD_GLOB  = '/dev/rbd/*/*'
HEADERS   = ('Pool', 'RBD name', 'Dev', 'Size',
             'r_iops', 'w_iops', 'rd MB/s', 'wr MB/s', 'r_rqm/s', '%r_rqm',
             'w_rqm/s', '%w_rqm','r_await', 'w_await', 'rareq-sz', 'wareq-sz',
             'aqu-sz',  '%util')    # queue length and utilization
DISCARD_H = ('d_iops',   'ds MB/s', 'd_rqm/s', '%d_rqm', 'd_await', 'dareq-sz')   # discard
SORT_FLDS = ('pool', 'rbd', 'dev',
             'r_io', 'r_mb', 'r_rqm', 'r_rqm_pct', 'r_wait', 'r_sz',
             'w_io', 'w_mb', 'w_rqm', 'w_rqm_pct', 'w_wait', 'w_sz',
             'queue', 'util', 'total_io')
             #'f/s',    'f_await',   # flushes


def parse_cmdline_args():
    """Parse command-line arguments."""
    args = ArgumentParser(description='RBD I/O statistics monitor')
    args.add_argument('inter', nargs='?', type=float, default=INTERVAL,
                      help='Continuous statistics interval (default: 0.0, i.e., one-shot)')
    args.add_argument('--pool', '-P', help='Which pool to monitor (default: all)')
    args.add_argument('--name', '-N', help='Which RBDs to monitor [regex] (default: all)')
    args.add_argument('--hist', '-H', type=int, default=1,
                      help='Statistics history entries to keep (default: 1, min: 1)')
    args.add_argument('--sort', '-S', choices=SORT_FLDS,
                      help='Which column to sort by (default: RBD name / pool name)')
    args.add_argument('--discard', '-D', action='store_true',
                      help='Include discard statistics in the output')
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

    if p.discard:   # add discard headers if requested
        global HEADERS
        HEADERS = (*HEADERS, *DISCARD_H)

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
    mapping: Dict[str, tuple[str, str]] = {'skipped': set()}
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
                    mapping['skipped'].add(dev)
                    continue

                blocks = int(read_oneline_file(f'/sys/block/{dev}/size').strip())
                mapping[dev] = (pool, rbd_name, blocks)

    return mapping


def read_stats(skipped: set[str]):
    """Read I/O statistics from /proc/diskstats for RBD devices."""
    stats: Dict[str, List[int]] = {'num_fields': 0}
    with open(DISKSTATS, encoding='utf-8') as f:
        for line in f:
            parts = line.split()
            num_fields = len(parts)
            dev = parts[2]

            if dev in skipped or not dev.startswith('rbd'):
                continue

            if stats['num_fields'] == 0:
                stats['num_fields'] = num_fields
            elif num_fields != stats['num_fields']:
                eprint(f'WARN: inconsistent number of fields in {DISKSTATS}: ',
                       f'expected {stats["num_fields"]}, got {num_fields}')
                continue
            elif num_fields < 14:
                continue

            # take the relevant fields
            stat_values = list(map(int, parts[3:]))
            stats[dev] = stat_values
    return stats


def sort_stats(stats: List[List[str]], key: str | None):
    """Sort the statistics data in-place based on the specified column."""
    match key:
        case None | 'rbd':
            stats.sort(key=lambda x: (x[1], x[0]))
        case 'pool':
            stats.sort(key=lambda x: (x[0], x[1]))
        case 'dev':
            stats.sort(key=lambda x: x[2])
        case 'r_io':
            stats.sort(key=lambda x: (int(ANSI_RX.sub('', x[4])), x[1]), reverse=True)
        case 'w_io':
            stats.sort(key=lambda x: (int(ANSI_RX.sub('', x[5])), x[1]), reverse=True)
        case 'r_mb':
            stats.sort(key=lambda x: (float(ANSI_RX.sub('', x[6])), x[1]), reverse=True)
        case 'w_mb':
            stats.sort(key=lambda x: (float(ANSI_RX.sub('', x[7])), x[1]), reverse=True)
        case 'r_rqm':
            stats.sort(key=lambda x: (int(ANSI_RX.sub('', x[8])), x[1]), reverse=True)
        case 'r_rqm_pct':
            stats.sort(key=lambda x: (float(x[9]), x[1]), reverse=True)
        case 'w_rqm':
            stats.sort(key=lambda x: (int(ANSI_RX.sub('', x[10])), x[1]), reverse=True)
        case 'w_rqm_pct':
            stats.sort(key=lambda x: (float(x[11]), x[1]), reverse=True)
        case 'r_wait':
            stats.sort(key=lambda x: (float(ANSI_RX.sub('', x[12])), x[1]), reverse=True)
        case 'w_wait':
            stats.sort(key=lambda x: (float(ANSI_RX.sub('', x[13])), x[1]), reverse=True)
        case 'r_sz':
            stats.sort(key=lambda x: (float(x[14]), x[1]), reverse=True)
        case 'w_sz':
            stats.sort(key=lambda x: (float(x[15]), x[1]), reverse=True)
        case 'queue':
            stats.sort(key=lambda x: (float(ANSI_RX.sub('', x[16])), x[1]), reverse=True)
        case 'util':
            stats.sort(key=lambda x: (float(ANSI_RX.sub('', x[17])), x[1]), reverse=True)
        case 'total_io':
            # combine read and write IOPS
            stats.sort(key=lambda x: (int(ANSI_RX.sub('', x[4])) + int(ANSI_RX.sub('', x[5])),
                                      x[1]), reverse=True)
        case _:
            return


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


# pylint: disable=too-many-locals
def parse_data(mapping: Dict, prev: Dict, now: Dict, delta_t: float, disc: bool):
    """Parse the current statistics data to compute I/O rates."""
    data = []
    for dev in sorted(mapping.keys()):
        if dev not in now:
            continue
        c = now[dev]
        p = prev.get(dev, [0] * len(c))

        rd_c = c[0] - p[0]
        rd_m = c[1] - p[1]
        rd_s = c[2] - p[2]
        rd_t = c[3] - p[3]
        wr_c = c[4] - p[4]
        wr_m = c[5] - p[5]
        wr_s = c[6] - p[6]
        wr_t = c[7] - p[7]
        io_t = c[9] - p[9]
        w_t = c[10] - p[10]

        r_s    = rd_c / delta_t if delta_t > 0 else 0.0
        w_s    = wr_c / delta_t if delta_t > 0 else 0.0
        r_mb_s = (rd_s / 2048.0) / delta_t if delta_t > 0 else 0.0
        w_mb_s = (wr_s / 2048.0) / delta_t if delta_t > 0 else 0.0

        rrqm_s = rd_m / delta_t if delta_t > 0 else 0.0
        wrqm_s = wr_m / delta_t if delta_t > 0 else 0.0
        p_rrqm = 100.0 * rd_m / (rd_m + rd_c) if (rd_m + rd_c) > 0 else 0.0
        p_wrqm = 100.0 * wr_m / (wr_m + wr_c) if (wr_m + wr_c) > 0 else 0.0

        r_await  = rd_t / rd_c if rd_c > 0 else 0.0
        w_await  = wr_t / wr_c if wr_c > 0 else 0.0
        rareq_sz = (rd_s / rd_c) / 2.0 if rd_c > 0 else 0.0
        wareq_sz = (wr_s / wr_c) / 2.0 if wr_c > 0 else 0.0

        aqu_sz = w_t / (delta_t * 1000.0) if delta_t > 0 else 0.0
        util   = 100.0 * io_t / (delta_t * 1000.0) if delta_t > 0 else 0.0

        row = [
            mapping[dev][0],        # Pool name
            GRN(mapping[dev][1]),   # RBD name
            dev,                    # /dev/rbdX
            humanize_size(mapping[dev][2] * 512),   # RBD exposes 512-byte blocks
            colorize(f"{r_s:.0f}", r_s, 10),        # read I/O per second
            colorize(f"{w_s:.0f}", w_s, 10),
            colorize(f"{r_mb_s:.1f}", r_mb_s),      # read MB/s
            colorize(f"{w_mb_s:.1f}", w_mb_s),
            colorize(f"{rrqm_s:.0f}", rrqm_s, 10),  # read requests merged per second
            f"{p_rrqm:.2f}",                        # read requests merged percentage
            colorize(f"{wrqm_s:.0f}", wrqm_s, 10),
            f"{p_wrqm:.2f}",
            colorize(f"{r_await:.1f}", r_await),    # read await time
            colorize(f"{w_await:.1f}", w_await),
            f"{rareq_sz:.1f}",                      # avg read request size
            f"{wareq_sz:.1f}",
            colorize(f"{aqu_sz:.1f}", aqu_sz),      # average queue size
            colorize(f"{util:.2f}", util),          # device utilization %
            ]

        if disc:    # add discard statistics if requested
            n = now['num_fields']
            disc_c = c[11] - p[11] if n >= 15 else 0
            disc_m = c[12] - p[12] if n >= 15 else 0
            disc_s = c[13] - p[13] if n >= 15 else 0
            disc_t = c[14] - p[14] if n >= 15 else 0

            d_s      = disc_c  / delta_t if delta_t > 0 else 0.0
            d_mb_s   = (disc_s / 2048.0) / delta_t if delta_t > 0 else 0.0
            drqm_s   = disc_m / delta_t if delta_t > 0 else 0.0
            p_drqm   = 100.0 * disc_m / (disc_m + disc_c) if (disc_m + disc_c) > 0 else 0.0
            d_await  = disc_t / disc_c if disc_c > 0 else 0.0
            dareq_sz = (disc_s / disc_c) / 2.0 if disc_c > 0 else 0.0

            row.extend([
                f"{d_s:.0f}",       # discards per second
                f"{d_mb_s:.1f}",    # ditto ...
                f"{drqm_s:.1f}",
                f"{p_drqm:.2f}",
                f"{d_await:.1f}",
                f"{dareq_sz:.1f}",
                #f"{f_s:.0f}",       # flushes per second
                #f"{f_await:.2f}",   # flush await time
                ])
        data.append(row)

        # maybe add flush statistics in the future
        #flush_c = c[15] - p[15] if n >= 17 else 0
        #flush_t = c[16] - p[16] if n >= 17 else 0
        #f_s = flush_c / delta_t if delta_t > 0 else 0.0
        #f_await = flush_t / flush_c if flush_c > 0 else 0.0
    return data


def main():
    global INTERVAL
    args       = parse_cmdline_args()
    history    = deque(maxlen=args.hist)
    continuous = args.inter > 0
    rbd_map    = build_mapping(patt=args.name)
    if len(rbd_map) < 2:    # at least one device + 'skipped' set
        print('No RBD devices found.')
        sys.exit(0)

    if continuous:
        INTERVAL = args.inter
        listener_t = threading.Thread(target=key_event_handler, daemon=True)
        listener_t.start()

    while True:
        now   = time.time()
        stats = read_stats(skipped=rbd_map['skipped'])

        if len(history) == 0:
            delta_t = float(read_oneline_file('/proc/uptime').split()[0])
            prev_time = now - delta_t   # assume first read is at system boot
            prev_stats = {dev: [0] * stats['num_fields'] for dev in stats}
            if continuous:
                history.append((prev_time, prev_stats))
        else:
            prev_time, prev_stats = history[-1]
            delta_t = max(now - prev_time, 1e-6)    # avoid division by zero

        # parse & display the data
        if not PAUSED:
            data = parse_data(mapping=rbd_map, prev=prev_stats, now=stats, delta_t=delta_t,
                              disc=args.discard)
            if continuous:
                clear_terminal()
            sort_stats(data, key=args.sort)
            print(simple_tabulate(data, headers=tuple(BOLD(h) for h in HEADERS)))
        if not continuous or QUITTING:
            break

        # keep history
        history.append((now, stats))

        # Sleep for the specified interval if continuous mode is enabled,
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
