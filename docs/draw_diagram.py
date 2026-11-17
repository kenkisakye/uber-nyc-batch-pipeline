"""Phase 2 architecture diagram, in the style of the approved Phase 1 draft."""
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import (Circle, Ellipse, FancyArrowPatch,
                                FancyBboxPatch, Rectangle)

OUT = sys.argv[1] if len(sys.argv) > 1 else "architecture.png"

NAVY = "#14243a"
GREY = "#4a5a6a"
BLUE = "#0b6fb3"
SKY = "#1784d0"
ORANGE = "#cf5310"
TEAL = "#16808a"
PURPLE = "#6a52a8"
GREEN = "#0f8a4f"
GOLD = "#f0b429"
GOLD_DARK = "#c27c0e"

plt.rcParams["font.family"] = "DejaVu Sans"
fig = plt.figure(figsize=(17, 8.9), dpi=150)
ax = fig.add_axes([0, 0, 1, 1])
ax.set_xlim(0, 170)
ax.set_ylim(0, 89)
ax.axis("off")
fig.patch.set_facecolor("white")


# ------------------------------------------------------------- helpers
def tile(x, y, label, color, size=4.2, fs=10):
    ax.add_patch(FancyBboxPatch((x - size / 2, y - size / 2), size, size,
                 boxstyle="round,pad=0,rounding_size=0.5",
                 fc=color, ec=color, zorder=6))
    ax.text(x, y, label, ha="center", va="center", color="white",
            fontsize=fs, fontweight="bold", zorder=7)


def clock(x, y, r=2.3):
    ax.add_patch(Circle((x, y), r, fc=SKY, ec=SKY, zorder=6))
    ax.plot([x, x], [y, y + r * 0.6], color="white", lw=2, zorder=7)
    ax.plot([x, x + r * 0.45], [y, y - r * 0.3], color="white", lw=2,
            zorder=7)


def cylinder(x, y, w=4.2, h=3.4):
    e = h * 0.32
    ax.add_patch(Rectangle((x - w / 2, y - h / 2), w, h, fc=GOLD_DARK,
                           ec="none", zorder=6))
    ax.add_patch(Ellipse((x, y - h / 2), w, e, fc=GOLD_DARK, ec="none",
                         zorder=6))
    for dy in (h * 0.05, -h * 0.3):
        ax.add_patch(Ellipse((x, y + dy), w, e, fc="none", ec=GOLD,
                             lw=1.2, zorder=7))
    ax.add_patch(Ellipse((x, y + h / 2), w, e, fc=GOLD, ec=GOLD_DARK,
                         lw=0.8, zorder=7))


def caption(x, y, title, sub=None, extra=None, fs=10.5):
    ax.text(x, y, title, ha="center", va="top", fontsize=fs,
            fontweight="bold", color=NAVY, zorder=8)
    yy = y - 2.4
    if sub:
        ax.text(x, yy, sub, ha="center", va="top", fontsize=7.6,
                color=GREY, zorder=8)
        yy -= 1.9
    if extra:
        ax.text(x, yy, extra, ha="center", va="top", fontsize=7.6,
                color=GREY, style="italic", zorder=8)


def path(points, color=NAVY, dashed=False, lw=1.5, arrow=True):
    xs, ys = zip(*points)
    ls = (0, (4, 3)) if dashed else "-"
    ax.plot(xs[:-1] + (xs[-1],) if not arrow else xs[:-1],
            ys[:-1] + (ys[-1],) if not arrow else ys[:-1],
            color=color, lw=lw, ls=ls, zorder=4, solid_capstyle="butt")
    if arrow:
        ax.add_patch(FancyArrowPatch(points[-2], points[-1],
                     arrowstyle="-|>", mutation_scale=12, color=color,
                     lw=lw, ls=ls, zorder=5, shrinkA=0, shrinkB=0))


def badge(x, y, n):
    ax.add_patch(Circle((x, y), 1.05, fc=GREEN, ec="white", lw=1.2,
                        zorder=9))
    ax.text(x, y - 0.05, str(n), ha="center", va="center", color="white",
            fontsize=8, fontweight="bold", zorder=10)


def frame(x0, y0, x1, y1, label, dashed=False, lw=1.6, lx=None):
    ax.add_patch(FancyBboxPatch((x0, y0), x1 - x0, y1 - y0,
                 boxstyle="round,pad=0,rounding_size=0.8", fc="none",
                 ec=BLUE, lw=lw, ls=(0, (4, 3)) if dashed else "-",
                 zorder=2))
    ax.text(lx if lx else x0 + 1.2, y1 - 1.6, label, ha="left",
            va="center", fontsize=10.5, fontweight="bold", color=BLUE,
            zorder=3)


# ------------------------------------------------------------- title
ax.text(2.5, 85.5, "Quarterly Uber pickup demand pipeline", fontsize=21,
        fontweight="bold", color=NAVY, va="center")
ax.text(2.6, 81.6, "About 4.5 million timestamped pickups  ·  six monthly "
        "files, April–September 2014  ·  two quarterly releases",
        fontsize=10.5, color=GREY, va="center")

# ------------------------------------------------------------- frames
frame(20, 22, 146, 77, "Local Docker Compose host")
frame(21.5, 23.5, 144.5, 74, "Private service network", dashed=True,
      lw=1.3)
frame(44, 25, 124, 46, "", lw=1.3)
ax.text(45.3, 28.6, "SeaweedFS buckets", fontsize=10.5, fontweight="bold",
        color=BLUE, va="center", zorder=3)
ax.text(45.3, 26.6, "2 volume servers · replication 001", fontsize=8,
        color=BLUE, va="center", zorder=3)

# ------------------------------------------------------------- nodes
Y = 56       # service row
YB = 38      # bucket row A
YB2 = 29.5   # bucket row B

tile(9, Y, "CSV", BLUE, fs=9.5)
caption(9, Y - 3.2, "Kaggle dataset", "Six monthly CSV files")

tile(31, Y, "</>", BLUE, fs=9)
caption(31, Y - 3.2, "Python ingestion", "python:3.11-slim-bookworm")

tile(66, Y, "✦", ORANGE, fs=11)
caption(66, Y - 3.2, "PySpark validation", "apache/spark:4.2.0 (Python)",
        "One month per run")

tile(102, Y, "✦", ORANGE, fs=11)
caption(102, Y - 3.2, "PySpark quarterly job", "apache/spark:4.2.0 (Python)",
        "After three validated months")

tile(136, 63, "S3", TEAL, fs=10)
caption(136, 59.8, "S3 gateway", "chrislusf/seaweedfs:4.17",
        "One access key per service")

tile(160, Y, "ML", PURPLE, fs=10)
caption(160, Y - 3.2, "Forecasting app", "External consumer")

clock(82, 68.5)
ax.text(86, 69.6, "APScheduler", fontsize=10.5, fontweight="bold",
        color=NAVY, va="center")
ax.text(86, 67.3, "Retries with back-off  ·  one run at a time",
        fontsize=7.6, color=GREY, va="center")

cylinder(49, YB)
caption(49, YB - 2.6, "Raw CSV", "File + checksum", fs=9.5)
cylinder(84, YB)
caption(84, YB - 2.6, "Parquet by month", "Grid cells, no exact points",
        fs=9.5)
cylinder(84, YB2 + 0.3)
ax.text(87.3, YB2 + 0.3, "Quarantine", fontsize=9.5, fontweight="bold",
        color=NAVY, va="center")
ax.text(87.3, YB2 - 1.8, "Rejected rows + reason", fontsize=7.6,
        color=GREY, va="center")
cylinder(117, YB)
caption(117, YB - 2.6, "Published release", "Staging, manifest last",
        fs=9.5)
cylinder(105, YB2 + 0.3)
ax.text(108.3, YB2 + 0.3, "Pipeline state", fontsize=9.5,
        fontweight="bold", color=NAVY, va="center")
ax.text(108.3, YB2 - 1.8, "Read by the scheduler", fontsize=7.6,
        color=GREY, va="center")

# ------------------------------------------------------------- data flow
path([(11.3, Y), (28.8, Y)])
badge(16.5, Y, 1)

path([(33.2, Y), (39, Y), (39, YB), (46.8, YB)])
badge(43, YB, 2)

path([(51.2, YB), (55, YB), (55, Y), (63.8, Y)])
badge(55, 43, 3)

path([(68.2, Y), (76.5, Y), (76.5, YB), (81.8, YB)])
path([(76.5, YB), (76.5, YB2 + 0.3), (81.8, YB2 + 0.3)])
badge(76.5, 43, 4)

path([(86.2, YB), (91, YB), (91, Y), (99.8, Y)])

path([(104.2, Y), (112, Y), (112, YB), (114.8, YB)])
badge(112, 43, 5)

path([(119.2, YB), (127, YB), (127, 63), (133.8, 63)])
path([(138.2, 63), (144, 63), (144, Y), (157.8, Y)])
badge(151, Y, 6)
ax.text(151, Y + 2.3, "Read-only key", ha="center", fontsize=8,
        color=GREY)

# ------------------------------------------------------------- control
path([(79.7, 68.5), (31, 68.5), (31, Y + 2.3)], color=BLUE, dashed=True,
     lw=1.3)
ax.text(32.2, 61.6, "Monthly load", fontsize=8, color=BLUE)
path([(79.9, 67.4), (66, 67.4), (66, Y + 2.3)], color=BLUE, dashed=True,
     lw=1.3)
ax.text(67.2, 61.6, "Monthly batch", fontsize=8, color=BLUE)
path([(82, 66.2), (82, 64.5), (102, 64.5), (102, Y + 2.3)], color=BLUE,
     dashed=True, lw=1.3)
ax.text(103.2, 61.6, "Quarterly release", fontsize=8, color=BLUE)


# ------------------------------------------------------------- notes
def notes(x, y, title, items, numbered=False, width=None):
    ax.text(x, y, title, fontsize=11, fontweight="bold", color=NAVY,
            va="center")
    yy = y - 2.7
    for i, t in enumerate(items, 1):
        if numbered:
            badge(x + 1.1, yy, i)
            ax.text(x + 3, yy, t, fontsize=8.6, color=NAVY, va="center")
        else:
            ax.text(x + 0.3, yy, "•  " + t, fontsize=8.6, color=NAVY,
                    va="center")
        yy -= 2.45


notes(2.5, 18, "Data flow", [
    "Load each monthly Kaggle CSV.",
    "Keep the original file and its checksum.",
    "Validate one month; round coordinates to grid cells.",
    "Store valid rows by month; rejected rows go to quarantine.",
    "Aggregate per cell, day, hour; check totals; publish manifest last.",
    "The app reads published releases through read-only S3.",
], numbered=True)

notes(68, 18, "Failure handling", [
    "Failed tasks are retried with back-off.",
    "Run state per month: reruns skip finished work.",
    "Rerunning a month overwrites it, so no duplicates.",
    "Totals must match: raw = valid + quarantined.",
    "A failed release keeps the previous one live.",
    "Every file is stored on both volume servers.",
])

notes(120, 18, "Maintenance", [
    "Pinned image versions in one .env file.",
    "Health checks, memory limits, log rotation.",
    "One command to start, check and reset.",
    "PySpark jobs in Git, run per month or quarter.",
    "Fixed schema and unit tests on sample rows.",
    "Rows and run time logged for every run.",
])

fig.savefig(OUT, dpi=150, facecolor="white")
print("saved", OUT)
