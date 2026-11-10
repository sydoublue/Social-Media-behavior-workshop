# Raw SNAP dumps

This directory is for local downloads. Its contents are gitignored except this README and `.gitkeep`. Do not commit the dumps: the typed graphs are large, and the study stores derived parquet outside git as well.

Download the three **typed** files per platform. The union file (`sx-<site>.txt.gz`) has no edge type; keep it only if you want an audit of the published total. Official pages and URLs also live in `configs/default.yaml`.

## MathOverflow (anchor, ingest this first)

Page: <https://snap.stanford.edu/data/sx-mathoverflow.html>

`python -m src.ingest --platform mathoverflow` downloads the three typed URLs from `configs/default.yaml` when the files are missing. The curls below are the same files.

```bash
mkdir -p data/raw/mathoverflow
curl -L --fail -o data/raw/mathoverflow/sx-mathoverflow-a2q.txt.gz \
  https://snap.stanford.edu/data/sx-mathoverflow-a2q.txt.gz
curl -L --fail -o data/raw/mathoverflow/sx-mathoverflow-c2q.txt.gz \
  https://snap.stanford.edu/data/sx-mathoverflow-c2q.txt.gz
curl -L --fail -o data/raw/mathoverflow/sx-mathoverflow-c2a.txt.gz \
  https://snap.stanford.edu/data/sx-mathoverflow-c2a.txt.gz
```

## Ask Ubuntu (replication, after MathOverflow is checked)

Page: <https://snap.stanford.edu/data/sx-askubuntu.html>

```bash
mkdir -p data/raw/askubuntu
curl -L --fail -o data/raw/askubuntu/sx-askubuntu-a2q.txt.gz \
  https://snap.stanford.edu/data/sx-askubuntu-a2q.txt.gz
curl -L --fail -o data/raw/askubuntu/sx-askubuntu-c2q.txt.gz \
  https://snap.stanford.edu/data/sx-askubuntu-c2q.txt.gz
curl -L --fail -o data/raw/askubuntu/sx-askubuntu-c2a.txt.gz \
  https://snap.stanford.edu/data/sx-askubuntu-c2a.txt.gz
```

## Super User (optional, later)

Page: <https://snap.stanford.edu/data/sx-superuser.html>

Use the same file pattern: `sx-superuser-a2q.txt.gz`, `sx-superuser-c2q.txt.gz`, `sx-superuser-c2a.txt.gz` from `https://snap.stanford.edu/data/`.

## Format

Each typed file is gzip-compressed text. Rows are whitespace-separated `SRC DST UNIXTS` with one interaction per line. The first line of `sx-mathoverflow-a2q.txt.gz` is a data row, `1 4 1254192988` (2009-09-29 02:56:28 UTC). Still skip blank lines and any line whose first non-whitespace character is `#`, in case another file carries a header.

Cite the source when you use the dumps: Paranjape, Benson, and Leskovec, “Motifs in Temporal Networks,” WSDM 2017. <https://doi.org/10.1145/3018661.3018731>
