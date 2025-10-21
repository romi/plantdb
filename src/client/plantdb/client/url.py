#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Robust server‑availability checker with built‑in SSRF / safety guards.
"""
import ipaddress
import os
import pathlib
import socket
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from typing import List
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from plantdb.commons.log import get_logger

logger = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

ALLOWED_SCHEMES: set[str] = {"http", "https"}  # Only HTTP(S)
ALLOWED_PORTS: set[int] = {80, 443}  # Optional – restrict to common ports
BLACKLIST_CACHE_FILE = pathlib.Path(tempfile.gettempdir()) / "github_blacklist_cache.txt"

# Private / non‑routable IP ranges – we refuse to connect to any of them
PRIVATE_NETWORKS: List[ipaddress._BaseNetwork] = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


def _load_whitelist_from_file() -> Optional[set[str]]:
    """
    Load allowed URLs from a specified environment variable file.

    This function reads a file path from an environment variable,
    parses it line by line, and extracts hostnames to create a set of allowed URLs.
    If any issues occur during the process, it returns `None`.

    Parameters
    ----------
    env_var : str
        The name of the environment variable that contains the file path.
    file_path : Optional[str]
        The path to the file containing the list of URLs. Defaults to the value from
        the specified environment variable.

    Returns
    -------
    Optional[set[str]]
        A set of allowed hostnames extracted from the file, or `None` if any errors occur.

    Notes
    -----
    - The function uses `urllib.parse.urlparse` to extract hostnames from URLs.
    - Lines starting with `#` are treated as comments and ignored.
    - Empty lines or lines without a valid hostname return an empty set.
    """
    env_var = "WHITELIST_URLS"
    file_path = os.getenv(env_var)
    if not file_path:
        return None
    path = pathlib.Path(file_path)
    if not path.is_file():
        return None
    try:
        hosts = set()
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parsed = urllib.parse.urlparse(line)
            # Fallback: if no scheme, try to parse as if it were a hostname.
            hostname = parsed.hostname or parsed.path
            if hostname:
                hosts.add(hostname.lower())
        return hosts or None
    except Exception:
        # Any error loading the file should default to no whitelist
        return None


WHITELIST: Optional[set[str]] = _load_whitelist_from_file()


def _download_and_cache_blacklist() -> Optional[pathlib.Path]:
    """
    Ensure the blacklist file is present locally and up‑to‑date.
    Returns the path to the cached file or ``None`` on failure.
    """
    cache_path = BLACKLIST_CACHE_FILE
    download_needed = True
    if cache_path.is_file():
        try:
            age = time.time() - cache_path.stat().st_mtime
            if age < 86400:  # less than 24 hours
                download_needed = False
        except Exception:
            download_needed = True
    if download_needed:
        # Use the GitHub API to fetch the list of split files (plain text)
        api_url = (
            "https://api.github.com/repos/Ultimate-Hosts-Blacklist/"
            "Ultimate.Hosts.Blacklist/contents/hosts"
        )
        try:
            api_resp = requests.get(api_url, timeout=10, headers={"Accept": "application/vnd.github.v3+json"})
            api_resp.raise_for_status()
            file_list = api_resp.json()
            if not isinstance(file_list, list):
                raise ValueError("Unexpected API response format")

            # Sort files by name to preserve order
            file_list.sort(key=lambda f: f.get("name", ""))

            total_size = sum(f.get("size", 0) for f in file_list)

            start_time = time.time()
            pbar = tqdm(total=total_size, unit='B', unit_scale=True,
                        desc='Downloading blacklist parts', leave=False) if total_size else None

            # Concatenate all parts directly into the cache file
            with open(cache_path, "wb") as out_f:
                for part in file_list:
                    dl_url = part.get("download_url")
                    if not dl_url:
                        continue
                    part_resp = requests.get(dl_url, timeout=10, stream=True)
                    part_resp.raise_for_status()
                    for chunk in part_resp.iter_content(chunk_size=8192):
                        if chunk:
                            out_f.write(chunk)
                            if pbar:
                                pbar.update(len(chunk))

            if pbar:
                pbar.close()

        except Exception as e:
            logger.error(f"Error loading GitHub blacklist: {e}")
            return None
    return cache_path if cache_path.is_file() else None


def _is_host_blacklisted(hostname: str) -> bool:
    """
    Check if a given hostname is listed in the blacklist.

    This function reads a locally cached blacklist file and checks if the
    provided hostname (with or without "www." prefix) is present in it.
    If the host is found in the blacklist, the function returns True,
    otherwise, it returns False. The hostname comparison is case-insensitive.

    Parameters
    ----------
    hostname : str
        The hostname to check against the blacklist file.

    Returns
    -------
    bool
        `True` if the hostname is listed in the blacklist, `False` otherwise.
        If there was an error accessing or reading the cache, it will return `False`.

    Examples
    --------
    >>> from plantdb.client.url import _is_host_blacklisted
    >>> _is_host_blacklisted("www.example.com")
    False
    >>> _is_host_blacklisted("localhost")
    True
    >>> _is_host_blacklisted("127.0.0.1")
    False

    Notes
    -----
    The function assumes that any issues with downloading or accessing the blacklist
    file are due to a network problem. It returns `False` in such cases, assuming
    the host is safe.

    See Also
    --------
    _download_and_cache_blacklist : Function to download and cache the blacklist.
    """
    cache_path = _download_and_cache_blacklist()
    if not cache_path:
        return False  # Cannot verify, assume safe

    host_lower = hostname.lower()
    if host_lower.startswith("www."):
        host_lower = host_lower[4:]

    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split()
                if len(parts) < 2:
                    continue
                blk_host = parts[1].lower()
                if blk_host.startswith("www."):
                    blk_host = blk_host[4:]
                if blk_host == host_lower:
                    logger.critical(f"Host '{host_lower}' is blacklisted!")
                    return True
    except Exception as e:
        logger.error(f"Error reading cached blacklist file: {e}")
    return False


# --------------------------------------------------------------------------- #
# Helper utilities
# --------------------------------------------------------------------------- #

def _is_private_ip(ip: str) -> bool:
    """
    Return True if *ip* is in a private / non‑routable network.

    Examples
    --------
    >>> from plantdb.client.url import _is_private_ip
    >>> _is_private_ip("172.16.31.10")
    True
    >>> _is_private_ip("127.0.0.1")
    True
    >>> _is_private_ip("localhost")
    True
    """
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        # Treat malformed IPs as private (safe‑by‑default)
        return True
    return any(addr in net for net in PRIVATE_NETWORKS)


def _resolve_public_ips(hostname: str) -> List[str]:
    """
    Resolve the given hostname to a list of its public IPv4/IPv6 addresses.

    This function queries DNS records for the specified hostname and filters out
    any private IP addresses.

    Parameters
    ----------
    hostname : str
        The hostname to resolve (e.g., 'example.com').

    Returns
    -------
    List[str]
        A list of public IPv4 addresses associated with the given hostname.
        If no valid public IPs are found, an empty list is returned.

    Examples
    --------
    >>> from plantdb.client.url import _resolve_public_ips
    >>> _resolve_public_ips('google.com')
    ['2a00:1450:4007:813::200e', '142.250.201.174']

    Notes
    -----
    It first attempt to use DNS over HTTPS endpoint for DNS resolution.
    Else, it relies on the `socket.getaddrinfo` method for DNS resolution.

    See Also
    --------
    socket.getaddrinfo : Get address information for a given hostname and service.
    """
    # Attempt DNS over HTTPS first using the provided resolver endpoint.
    try:
        # DNS over HTTPS endpoint that accepts DNS queries via HTTP GET
        doh_url = "https://protective.joindns4.eu/dns-query"
        headers = {
            "Accept": "application/dns-json",
            "User-Agent": "plantdb-client/1.0",
        }
        params = {"name": hostname, "type": "A"}
        resp = requests.get(doh_url, headers=headers, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        ips = []
        for answer in data.get("Answer", []):
            # The answer data for type A is an IPv4 address
            if answer.get("type") == 1:  # A record
                ips.append(answer.get("data"))
        # Filter out private IPs
        public_ips = [ip for ip in ips if not _is_private_ip(ip)]
        if public_ips:
            return public_ips
    except Exception:
        # Fall back to the system resolver if DOH fails
        pass

    try:
        addrinfo = socket.getaddrinfo(hostname, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return []

    public_ips = set()
    for entry in addrinfo:
        ip = entry[4][0]
        if not _is_private_ip(ip):
            public_ips.add(ip)
    return list(public_ips)


def _validate_host(hostname: str, allow_private_ip: bool = False) -> bool:
    """
    Validate the hostname based on given criteria.

    This function checks if a hostname is valid by verifying it against a whitelist,
    blacklist, and ensuring it has at least one public IP unless private IPs are allowed.

    Parameters
    ----------
    hostname : str
        The hostname to validate.
    allow_private_ip : bool, optional
        Flag indicating whether to allow private IPs. Default is False.

    Returns
    -------
    bool
        True if the hostname is valid according to the criteria,
        False otherwise.

    Examples
    --------
    >>> from plantdb.client.url import _validate_host
    >>> _validate_host("example.com")
    True
    >>> _validate_host("localhost")
    >>> _validate_host("localhost", allow_private_ip=True)
    True

    Notes
    -----
    This function relies on the global `WHITELIST` variable and private functions
    `_is_host_blacklisted()` and `_resolve_public_ips()`.

    See Also
    --------
    _is_host_blacklisted : Check if a host is blacklisted.
    _resolve_public_ips : Resolve public IPs for a given hostname.

    """
    if WHITELIST:
        if hostname not in WHITELIST:
            return False
    else:
        if _is_host_blacklisted(hostname):
            return False

    if allow_private_ip:
        return True

    public_ips = _resolve_public_ips(hostname)
    return bool(public_ips)  # at least one public IP

def _parse_url(url: str, allow_private_ip: bool = False) -> Optional[urllib.parse.ParseResult]:
    """
    Parse a URL string into components and validate it according to specified rules.

    This function attempts to parse the given URL string using Python's
    ``urllib.parse.urlparse`` method. It then applies several checks on
    the parsed result, including scheme validation, hostname presence,
    optional port validation, and host validation with optional private IP
    allowance.

    Parameters
    ----------
    url : str
        The URL string to be parsed.
    allow_private_ip : bool, optional
        Whether to allow private IP addresses. Default is False.

    Returns
    -------
    parsed : Optional[urllib.parse.ParseResult]
        A ``ParseResult`` object representing the components of the URL,
        or None if any validation step fails.

    Examples
    --------
    >>> from plantdb.client.url import _parse_url
    >>> _parse_url("http://example.com")
    ParseResult(scheme='http', netloc='example.com', path='', params='', query='', fragment='')
    >>> _parse_url("file:///etc/passwd", allow_private_ip=True)
    None

    Notes
    -----
    Private IP addresses are allowed only if ``allow_private_ip`` is set to True.
    Any URL with a file scheme or an invalid hostname will return None.

    See Also
    --------
    urllib.parse.urlparse : Parse a URL string into components.
    """
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return None

    # Scheme check
    if parsed.scheme not in ALLOWED_SCHEMES:
        return None

    # No file:// or other dangerous schemes
    if parsed.scheme == "file":
        return None

    # Hostname must exist
    if not parsed.hostname:
        return None

    # Optional port check
    if parsed.port and parsed.port not in ALLOWED_PORTS:
        return None

    # Host validation (whitelist/blacklist + DNS resolution)
    if not _validate_host(parsed.hostname, allow_private_ip=allow_private_ip):
        return None

    return parsed


# --------------------------------------------------------------------------- #
# Main checker
# --------------------------------------------------------------------------- #

@dataclass
class ServerCheckResult:
    """Class containing information about the server's response."""
    url: str
    status_code: int
    ok: bool
    final_url: str


def is_server_available(
        url: str,
        timeout: float = 5.0,
        max_redirects: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 0.3,
        allow_private_ip: bool = False,
) -> ServerCheckResult:
    """
    Check if a server is available by making an HTTP request to the given URL.

    Parameters
    ----------
    url : str
        The URL of the server to check.
    timeout : float, optional
        The maximum number of seconds to wait for a response. Default is 5.0.
    max_redirects : int, optional
        The maximum number of redirects to follow. Default is 2.
    max_retries : int, optional
        The maximum number of retries in case of failure. Default is 3.
    backoff_factor : float, optional
        A factor to multiply the delay between retry attempts. Default is 0.3.
    allow_private_ip : bool, optional
        If True, the checker will accept URLs whose resolved IPs are private or non‑routable.
        Default is False.

    Returns
    -------
    ServerCheckResult
        An object containing details about the server check result.

        - url (str): The original URL that was checked.
        - status_code (int): The HTTP status code received from the server.
        - ok (bool): True if the server responded with a successful status code,
          False otherwise.
        - final_url (str): The final URL after all redirects have been followed.

    Examples
    --------
    >>> from plantdb.client.url import is_server_available
    >>> result = is_server_available("https://example.com")
    >>> print(result.ok)
    True
    >>> print(result.url)
    https://example.com
    >>> result = is_server_available("https://127.0.0.1/plantdb")
    >>> print(result.ok)
    False
    >>> result = is_server_available("https://127.0.0.1/plantdb", allow_private_ip=True)
    >>> print(result.ok)
    Notes
    -----
    This function uses the `requests` library to perform HTTP requests. It handles
    retries and redirects according to the parameters provided.

    See Also
    --------
    requests.Session : The class used for sending HTTP requests.
    urllib.parse.urljoin : Used to resolve relative URLs in redirects.
    """
    parsed = _parse_url(url, allow_private_ip=allow_private_ip)
    if not parsed:
        logger.warning(f"Invalid URL detected: {url}")
        return ServerCheckResult(url=url, status_code=0, ok=False, final_url=url)

    session = requests.Session()
    retry = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods={"GET", "HEAD"},
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    current_url = url
    for _ in range(max_redirects + 1):
        try:
            resp = session.get(
                current_url,
                timeout=timeout,
                verify=True,  # TLS cert verification
                allow_redirects=False,  # We handle redirects manually
                stream=True,  # Do not download the body unless we need to
            )
        except requests.RequestException:
            return ServerCheckResult(url=url, status_code=0, ok=False, final_url=current_url)

        # If we get a redirect, validate the target URL
        if 300 <= resp.status_code < 400:
            location = resp.headers.get("Location")
            if not location:
                return ServerCheckResult(url=url, status_code=resp.status_code, ok=False, final_url=current_url)

            # Resolve relative URLs
            next_url = urllib.parse.urljoin(current_url, location)

            # Safety check on the next hop
            if not _parse_url(next_url, allow_private_ip=allow_private_ip):
                return ServerCheckResult(url=url, status_code=resp.status_code, ok=False, final_url=next_url)

            current_url = next_url
            continue

        # Not a redirect – return the result
        return ServerCheckResult(
            url=url,
            status_code=resp.status_code,
            ok=200 <= resp.status_code < 400,
            final_url=current_url,
        )

    # Too many redirects
    return ServerCheckResult(url=url, status_code=0, ok=False, final_url=current_url)