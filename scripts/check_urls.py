from __future__ import annotations
import argparse
import asyncio
import contextlib
import csv
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Iterable, Optional, Tuple, List, Dict, Any

import aiohttp
import pandas as pd

# --- Configuration helpers --------------------------------------------------

DEFAULT_INPUT = os.environ.get("URL_INPUT_PATH", "data/combined_master_with_urls.xlsx")
DEFAULT_SHEET = os.environ.get("URL_INPUT_SHEET", None)
DEFAULT_URL_COLS = os.environ.get("URL_COLUMNS", None)  # comma-separated
DEFAULT_OUTPUT_DIR = os.environ.get("URL_OUTPUT_DIR", "artifacts")
DEFAULT_CONCURRENCY = int(os.environ.get("URL_CONCURRENCY", "100"))
DEFAULT_TIMEOUT = float(os.environ.get("URL_TIMEOUT", "12"))
DEFAULT_RETRIES = int(os.environ.get("URL_RETRIES", "2"))
DEFAULT_ENRICH = os.environ.get("URL_WRITE_ENRICHED", "1") == "1"
DEFAULT_USER_AGENT = os.environ.get(
    "URL_USER_AGENT",
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
)

# Sharding: shard_index in [0..shard_total-1]
SHARD_INDEX = int(os.environ.get("URL_SHARD_INDEX", os.environ.get("MATRIX_SHARD", "0")))
SHARD_TOTAL = int(os.environ.get("URL_SHARD_TOTAL", os.environ.get("MATRIX_TOTAL", "1")))

# --- URL utilities ----------------------------------------------------------

URL_RE = re.compile(r"https?://[^\s]+", re.IGNORECASE)


def normalize_url(u: str) -> Optional[str]:
    if not isinstance(u, str):
        return None
    u = u.strip().strip('"').strip("'")
    if not u:
        return None
    if u.lower().startswith(("http://", "https://")):
        return u
    return None


@dataclass
class UrlResult:
    url: str
    status: Optional[int]
    reason: str
    method: str
    final_url: str
    elapsed_ms: int
    attempts: int
    is_broken: bool
    suggested_alternative: Optional[str]


# --- Core checker -----------------------------------------------------------

class UrlChecker:
    def __init__(
        self,
        *,
        concurrency: int = DEFAULT_CONCURRENCY,
        timeout: float = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
        user_agent: str = DEFAULT_USER_AGENT,
        progress_every: int = 200,
    ) -> None:
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = aiohttp.ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout)
        self.retries = retries
        self.headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml,application/pdf;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
        self.progress_every = progress_every
        self._checked = 0
        self._total = 0

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(headers=self.headers, timeout=self.timeout, connector=connector)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    def _classify(self, status: Optional[int], reason: str) -> bool:
        # True means broken
        if status is None:
            # network-level failure
            return True if ("dns" in reason or "connect" in reason or "ssl" in reason or "timeout" in reason) else True
        if 200 <= status <= 399:
            return False
        if status in {401, 403, 429}:
            # reachable but blocked/rate-limited → not broken for our purposes
            return False
        if status in {404, 410, 451}:
            return True
        if 500 <= status <= 599:
            return True
        # default conservative
        return True

    async def check_one(self, url: str) -> UrlResult:
        async with self.semaphore:
            attempts = 0
            last_exc: Optional[BaseException] = None
            method_used = "HEAD"
            start_all = time.perf_counter()
            final_url = url
            status: Optional[int] = None
            reason = ""

            for attempt in range(self.retries + 1):
                attempts = attempt + 1
                try:
                    # First try HEAD
                    method_used = "HEAD"
                    async with self.session.head(url, allow_redirects=True) as resp:
                        status = resp.status
                        final_url = str(resp.url)
                        reason = resp.reason or ""
                        # If HEAD not allowed or unhelpful, try GET
                        if status in (405, 501) or (status == 200 and resp.headers.get("Content-Length") is None):
                            method_used = "GET"
                            async with self.session.get(url, allow_redirects=True) as gresp:
                                status = gresp.status
                                final_url = str(gresp.url)
                                reason = gresp.reason or reason or ""
                    break  # success path (response obtained)
                except aiohttp.ClientResponseError as e:
                    status = e.status
                    reason = e.message or type(e).__name__
                    if status in (429,) or 500 <= status <= 599:
                        await asyncio.sleep(min(5, 0.5 * (2 ** attempt)))
                        continue
                    break
                except (aiohttp.ServerDisconnectedError, aiohttp.ClientOSError, aiohttp.ClientConnectorError) as e:
                    last_exc = e
                    status = None
                    reason = type(e).__name__.lower()
                    await asyncio.sleep(min(5, 0.5 * (2 ** attempt)))
                    continue
                except asyncio.TimeoutError as e:
                    last_exc = e
                    status = None
                    reason = "timeout"
                    await asyncio.sleep(min(5, 0.5 * (2 ** attempt)))
                    continue
                except aiohttp.TooManyRedirects as e:
                    last_exc = e
                    status = 310  # pseudo
                    reason = "too_many_redirects"
                    break
                except Exception as e:  # noqa: BLE001
                    last_exc = e
                    status = None
                    reason = type(e).__name__.lower()
                    break

            elapsed_ms = int((time.perf_counter() - start_all) * 1000)
            suggested = None
            if status in {404, 410, 451} or reason in {"too_many_redirects"}:
                suggested = await self._suggest_alternative(url)

            result = UrlResult(
                url=url,
                status=status,
                reason=reason,
                method=method_used,
                final_url=final_url,
                elapsed_ms=elapsed_ms,
                attempts=attempts,
                is_broken=self._classify(status, reason),
                suggested_alternative=suggested,
            )

            # progress logging for Actions log
            self._checked += 1
            if self._total and self._checked % self.progress_every == 0:
                pct = 100.0 * self._checked / self._total
                print(f"[progress] {self._checked}/{self._total} ({pct:.1f}%) done...", flush=True)

            return result

    async def _suggest_alternative(self, url: str) -> Optional[str]:
        # minimal, safe, and fast heuristics; no external search
        candidates: List[str] = []
        try:
            if url.startswith("http://"):
                candidates.append("https://" + url[len("http://") :])
            if url.startswith("https://"):
                candidates.append("http://" + url[len("https://") :])
            # strip query/fragment
            base = re.split(r"[?#]", url)[0]
            if base != url:
                candidates.append(base)
            # parent path
            if "/" in base[8:]:  # ignore scheme
                parent = base.rsplit("/", 1)[0]
                candidates.append(parent)
        except Exception:
            return None

        for cand in candidates:
            try:
                async with self.session.head(cand, allow_redirects=True) as resp:
                    if 200 <= resp.status <= 399:
                        return str(resp.url)
                async with self.session.get(cand, allow_redirects=True) as resp:
                    if 200 <= resp.status <= 399:
                        return str(resp.url)
            except Exception:
                continue
        return None

    async def run(self, urls: List[str]) -> List[UrlResult]:
        self._total = len(urls)
        print(f"[start] checking {self._total} URLs with concurrency={self.semaphore._value}, retries={self.retries}")
        tasks = [asyncio.create_task(self.check_one(u)) for u in urls]
        results: List[UrlResult] = []
        for coro in asyncio.as_completed(tasks):
            res = await coro
            results.append(res)
        print("[done] all checks completed")
        return results


# --- Data IO ----------------------------------------------------------------

def read_table(path: str, sheet: Optional[str] = None) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet)
    elif ext in {".csv", ".tsv"}:
        sep = "," if ext == ".csv" else "\t"
        return pd.read_csv(path, sep=sep)
    else:
        raise ValueError(f"Unsupported input extension: {ext}")


def detect_url_columns(df: pd.DataFrame) -> List[str]:
    name_hits = [c for c in df.columns if re.search(r"url|link", str(c), re.IGNORECASE)]
    if name_hits:
        return name_hits
    # fallback: scan object columns for http(s) values
    cols: List[str] = []
    for c in df.columns:
        if df[c].dtype == object:
            sample = df[c].dropna().astype(str).head(200).tolist()
            if any(s.strip().lower().startswith(("http://", "https://")) for s in sample):
                cols.append(c)
    return cols


def extract_urls(df: pd.DataFrame, url_cols: Optional[List[str]]) -> Tuple[List[str], Dict[str, List[Tuple[int, str]]]]:
    mapping: Dict[str, List[Tuple[int, str]]] = {}
    urls: List[str] = []
    if not url_cols:
        url_cols = detect_url_columns(df)
    if not url_cols:
        # brute force search across all cells
        for idx, row in df.iterrows():
            for c in df.columns:
                cell = row[c]
                if isinstance(cell, str):
                    for m in URL_RE.finditer(cell):
                        u = normalize_url(m.group(0))
                        if u:
                            mapping.setdefault(u, []).append((idx, c))
                            urls.append(u)
    else:
        for c in url_cols:
            for idx, val in df[c].items():
                u = normalize_url(val)
                if u:
                    mapping.setdefault(u, []).append((idx, c))
                    urls.append(u)
    # de-duplicate but keep order
    seen = set()
    deduped: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped, mapping


def enrich_dataframe(df: pd.DataFrame, mapping: Dict[str, List[Tuple[int, str]]], results: Dict[str, UrlResult]) -> pd.DataFrame:
    out = df.copy()
    # For each referenced column, add status and final_url columns
    touched_cols = {col for pairs in mapping.values() for (_, col) in pairs}
    for col in sorted(touched_cols):
        status_col = f"{col}_status"
        final_col = f"{col}_final_url"
        broken_col = f"{col}_is_broken"
        out[status_col] = pd.NA
        out[final_col] = pd.NA
        out[broken_col] = pd.NA
    for url, pairs in mapping.items():
        res = results.get(url)
        if not res:
            continue
        for (idx, col) in pairs:
            out.at[idx, f"{col}_status"] = res.status
            out.at[idx, f"{col}_final_url"] = res.final_url
            out.at[idx, f"{col}_is_broken"] = res.is_broken
    return out


# --- CLI --------------------------------------------------------------------

def shard_list(items: List[str], shard_index: int, shard_total: int) -> List[str]:
    if shard_total <= 1:
        return items
    return [u for i, u in enumerate(items) if i % shard_total == shard_index]


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Fast URL health checker for large spreadsheets")
    p.add_argument("--input", default=DEFAULT_INPUT, help="Path to Excel/CSV file")
    p.add_argument("--sheet", default=DEFAULT_SHEET, help="Excel sheet name (optional)")
    p.add_argument("--url-columns", default=DEFAULT_URL_COLS, help="Comma-separated list of URL column names (optional)")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Directory to write result files")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    p.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    p.add_argument("--shard-index", type=int, default=SHARD_INDEX)
    p.add_argument("--shard-total", type=int, default=SHARD_TOTAL)
    p.add_argument("--no-enrich", action="store_true", help="Skip writing enriched Excel with status columns")
    args = p.parse_args(argv)

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"[config] input={args.input} sheet={args.sheet} url_columns={args.url_columns}")
    print(f"[config] concurrency={args.concurrency} timeout={args.timeout}s retries={args.retries}")
    print(f"[config] shard {args.shard_index+1}/{args.shard_total}")

    df = read_table(args.input, args.sheet)

    url_cols = [c.strip() for c in args.url_columns.split(",")] if args.url_columns else None

    urls, mapping = extract_urls(df, url_cols)

    if not urls:
        print("No URLs found in the input file.")
        return 2

    shard_urls = shard_list(urls, args.shard_index, args.shard_total)

    print(f"[info] total unique URLs: {len(urls)}; this shard will check: {len(shard_urls)}")

    async def _run():
        async with UrlChecker(concurrency=args.concurrency, timeout=args.timeout, retries=args.retries) as checker:
            results = await checker.run(shard_urls)
            return results

    results_list: List[UrlResult] = asyncio.run(_run())

    # Save per-shard results
    out_all_csv = os.path.join(args.output_dir, f"url_check_results_shard{args.shard_index}.csv")
    with open(out_all_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "url",
                "status",
                "reason",
                "method",
                "final_url",
                "elapsed_ms",
                "attempts",
                "is_broken",
                "suggested_alternative",
            ],
        )
        w.writeheader()
        for r in results_list:
            w.writerow(asdict(r))

    print(f"[write] {out_all_csv}")

    # Broken-only CSV
    broken = [r for r in results_list if r.is_broken]
    out_broken_csv = os.path.join(args.output_dir, f"broken_urls_shard{args.shard_index}.csv")
    with open(out_broken_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "url",
                "status",
                "reason",
                "final_url",
                "suggested_alternative",
                "elapsed_ms",
                "attempts",
            ],
        )
        w.writeheader()
        for r in broken:
            w.writerow(
                {
                    "url": r.url,
                    "status": r.status,
                    "reason": r.reason,
                    "final_url": r.final_url,
                    "suggested_alternative": r.suggested_alternative,
                    "elapsed_ms": r.elapsed_ms,
                    "attempts": r.attempts,
                }
            )

    print(f"[write] {out_broken_csv} (broken: {len(broken)})")

    # Enriched Excel (optional, only on shard 0 to keep it simple)
    if not args.no_enrich and args.shard_index == 0:
        res_map: Dict[str, UrlResult] = {r.url: r for r in results_list}
        enriched = enrich_dataframe(df, mapping, res_map)
        out_xlsx = os.path.join(args.output_dir, "enriched_with_status.xlsx")
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            enriched.to_excel(writer, index=False)
        print(f"[write] {out_xlsx}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
