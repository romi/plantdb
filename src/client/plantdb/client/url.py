#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Robust server‑availability checker with built‑in SSRF / safety guards.
"""
import ipaddress
import json
import os
import socket
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List
from typing import Optional
from urllib.error import URLError

import urllib3
from ada_url import URL
from ada_url import check_url
from ada_url import join_url
from plantdb.commons.log import get_logger
from tqdm import tqdm
from urllib3.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError
from urllib3.exceptions import NewConnectionError
from urllib3.exceptions import TimeoutError
from urllib3.util import create_urllib3_context
from urllib3.util.retry import Retry

logger = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

ALLOWED_SCHEMES: set[str] = {"http", "https"}  # Only HTTP(S)
BLACKLIST_CACHE_FILE = Path(tempfile.gettempdir()) / "github_blacklist_cache.txt"

# Private / non‑routable IP ranges - we refuse to connect to any of them
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


def _load_whitelist_from_file() -> Optional[set[str]] :
    """Load allowed URLs from a specified environment variable file.

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
    - Empty lines or comment lines (starting with `#`) are ignored.
    - Lines without a valid hostname (according to Ada) are ignored.
    """
    env_var = "WHITELIST_URLS"  # Environment variable name containing the file path
    file_path = os.getenv(env_var)  # Get the file path from environment variable
    if not file_path:  # If no file path is provided, return None
        return None

    path = Path(file_path)  # Create a Path object for the file
    if not path.is_file():  # Check if the path is an existing file
        return None

    try:
        hosts = set()  # Initialize an empty set to store hostnames
        # Read and process each line of the file
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):  # Skip empty lines and comments
                continue

            parsed = URL(line)  # Parse the line as a URL object
            hostname = parsed.hostname  # Extract hostname from URL
            if hostname:
                hosts.add(hostname.lower())  # Add lowercase hostname to set
        return hosts or None

    except Exception:
        # Any error loading the file should default to no whitelist
        return None


WHITELIST: Optional[set[str]] = _load_whitelist_from_file()


def _download_and_cache_blacklist(force: bool = False) -> Optional[Path] :
    """Ensure the blacklist file is present locally and up‑to‑date.
    Returns the path to the cached file or ``None`` on failure.

    Parameters
    ----------
    force : bool, optional
        Force download of the blacklist file, even if present.

    Examples
    --------
    >>> from plantdb.client.url import _download_and_cache_blacklist
    >>> blacklist_path = _download_and_cache_blacklist(force=True)
    >>> print(blacklist_path)
    /tmp/github_blacklist_cache.txt
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

    if force:
        download_needed = True

    if download_needed:
        # Use the GitHub API to fetch the list of split files (plain text)
        api_url = (
            "https://api.github.com/repos/Ultimate-Hosts-Blacklist/"
            "Ultimate.Hosts.Blacklist/contents/hosts"
        )

        # Create a PoolManager instance
        http = urllib3.PoolManager()

        try:
            # Fetch the API response
            api_resp = http.request(
                'GET',
                api_url,
                headers={"Accept": "application/vnd.github.v3+json"},
                timeout=urllib3.Timeout(total=10)
            )

            # Check response status
            if api_resp.status != 200:
                raise ValueError(f"API request failed with status {api_resp.status}")

            # Parse JSON response
            file_list = json.loads(api_resp.data.decode('utf-8'))

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

                    # Download each part
                    part_resp = http.request('GET', dl_url, preload_content=False, timeout=urllib3.Timeout(total=10))

                    try:
                        for chunk in part_resp.stream(8192):
                            out_f.write(chunk)
                            if pbar:
                                pbar.update(len(chunk))
                    finally:
                        part_resp.release_conn()

            if pbar:
                pbar.clear()  # clear the space allocated to the progress bar
                pbar.close()

        except (HTTPError, URLError) as e:
            logger.error(f"Error downloading GitHub blacklist: {e}")
            return None
        except (json.JSONDecodeError) as e:
            logger.error(f"Error loading GitHub blacklist: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error loading GitHub blacklist: {e}")
            return None

    return cache_path if cache_path.is_file() else None


def _is_host_blacklisted(hostname: str) -> bool :
    """Check if a given hostname is listed in the blacklist.

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
        return False

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

def _is_private_ip(ip: str) -> bool :
    """Return True if *ip* is in a private / non‑routable network.

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


def _resolve_public_ips(hostname: str) -> List[str] :
    """Resolve the given hostname to a list of its public IPv4/IPv6 addresses.

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
        May also contain IPv6 addresses if using the system DNS resolver.
        If no valid public IPs are found, an empty list is returned.

    Examples
    --------
    >>> from plantdb.client.url import _resolve_public_ips
    >>> _resolve_public_ips('google.com')
    ['142.250.201.174']

    Notes
    -----
    It first attempt to use DNS over HTTPS endpoint for DNS resolution.
    Else, it relies on the `socket.getaddrinfo` method for DNS resolution.

    See Also
    --------
    socket.getaddrinfo : Get address information for a given hostname and service.
    """
    # Attempt DNS over HTTPS first using the provided resolver endpoint.
    # Create a PoolManager for HTTP requests
    http = urllib3.PoolManager(
        headers={
            "Accept": "application/dns-json",  # Request DNS response in JSON format
            "User-Agent": "plantdb-client/1.0",
        },
        retries=urllib3.Retry(total=1, backoff_factor=0.1)  # Single retry with 0.1s delay
    )

    # Primary resolution method: DNS over HTTPS (DoH) for enhanced security & privacy
    try:
        # Use Cloudflare DNS over HTTPS resolver [[1]](https://developers.cloudflare.com/1.1.1.1/encryption/dns-over-https/)
        doh_url = "https://cloudflare-dns.com/dns-query"
        url = f"{doh_url}?name={hostname}&type=A"
        # Execute DNS query with 5 second timeout
        resp = http.request('GET', url, timeout=5.0)

        # Validate HTTP response status
        if resp.status != 200:
            raise ValueError(f"HTTP request failed with status {resp.status}")

        # Deserialize JSON response from DoH server
        data = json.loads(resp.data.decode('utf-8'))

        # Extract IP addresses from DNS answers
        ips = []
        for answer in data.get("Answer", []):
            # Type 1 indicates A record (IPv4 address)
            if answer.get("type") == 1:
                ips.append(answer.get("data"))

        # Remove private/internal IP addresses from results
        public_ips = [ip for ip in ips if not _is_private_ip(ip)]
        # Return immediately if DoH found public IPs
        if public_ips:
            return public_ips
    except Exception:
        logger.error(f"Error resolving public IPs with DNS over HTTPS for: '{hostname}'.")
        # Fallback to system resolver
        pass

    # Fallback resolution method: system DNS resolver
    try:
        logger.info("Fallback to system DNS resolver...")
        # Query all TCP-compatible addresses for hostname
        addrinfo = socket.getaddrinfo(hostname, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        # Return empty list if hostname cannot be resolved
        return []
    else:
        # Remove private/internal IP addresses from results
        public_ips = set()
        # Iterate through resolved address information
        for entry in addrinfo:
            ip = entry[4][0]  # Extract IP address from socket address tuple
            if not _is_private_ip(ip):
                public_ips.add(ip)  # Add only non-private IPs

    return list(public_ips)


def _validate_hostname(hostname: str, allow_private_ip: bool = False) -> bool :
    """Validate the hostname based on given criteria.

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
    >>> from plantdb.client.url import _validate_hostname
    >>> _validate_hostname("google.com")
    >>> _validate_hostname("example.com")
    True
    >>> _validate_hostname("localhost")
    >>> _validate_hostname("localhost", allow_private_ip=True)
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
    # Allow localhost/127.0.0.1 during development when private IPs are allowed
    if allow_private_ip and hostname.lower() in ("localhost", "127.0.0.1"):
        return True

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


def _parse_url(url: str) -> Optional[str]:
    """Parse a URL string into components.

    Parameters
    ----------
    url : str
        The URL string to be parsed.

    Returns
    -------
    Optional[str]
        The parsed URL, or `None` if any validation step fails.

    Examples
    --------
    >>> from plantdb.client.url import _parse_url
    >>> url = _parse_url("http://example.com")
    >>> print(url)
    http://example.com/
    """
    try:
        parsed_url = URL(url)
    except Exception:
        return None

    # Validate URL structure with Ada-url
    if not check_url(parsed_url.href):
        logger.error(f"URL '{url}' is not a valid URL!")
        return None

    # Scheme check
    if parsed_url.protocol.rstrip(":") not in ALLOWED_SCHEMES:
        logger.error(f"URL '{url}' is not an allowed protocol!")
        return None

    # Hostname must exist
    if not parsed_url.hostname:
        logger.error(f"URL '{url}' is not a valid hostname!")
        return None

    return parsed_url.href


# --------------------------------------------------------------------------- #
# Main checker
# --------------------------------------------------------------------------- #


def _build_ssl_context(
        cert_path: Optional[Path],
        verify_ssl: bool,
) -> tuple[Optional[urllib3.util.ssl_.SSLContext], Optional[str], str]:
    """Create an SSL context and determine the certificate requirements for the connection pool.

    Parameters
    ----------
    cert_path:
        Path to a custom CA bundle or directory containing certificates.
    verify_ssl:
        Whether SSL verification should be performed.

    Returns
    -------
    Optional[urllib3.util.ssl_.SSLContext]
        ``urllib3.util.ssl_.SSLContext`` instance or ``None`` if no custom context is needed.
    Optional[str]
        Path to a CA bundle to be passed to ``urllib3.PoolManager``.
        ``None`` indicates that the default system certificates should be used.
    str
        SSL certificate verification mode accepted by ``urllib3.PoolManager``.
        ``'CERT_REQUIRED'`` or ``'CERT_NONE'``.
    """
    ssl_context: Optional[urllib3.util.ssl_.SSLContext] = None
    ca_certs: Optional[str] = None
    cert_reqs = "CERT_REQUIRED"

    # Sets up SSL context and CA certificates for requests
    if cert_path is not None:
        ssl_context = create_urllib3_context()
        if not verify_ssl:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = urllib3.util.ssl_.CERT_NONE
            cert_reqs = "CERT_NONE"

        if cert_path.is_file():
            ssl_context.load_verify_locations(cafile=str(cert_path))
            ca_certs = str(cert_path)
        elif cert_path.is_dir():
            cert_file = cert_path / "cert.pem"
            if cert_file.exists():
                ssl_context.load_verify_locations(cafile=str(cert_file))
                ca_certs = str(cert_file)
            else:
                ssl_context.load_verify_locations(capath=str(cert_path))
                ca_certs = str(cert_path)

    if not verify_ssl:
        cert_reqs = "CERT_NONE"
        ca_certs = None

    return ssl_context, ca_certs, cert_reqs


def _configure_http_pool(
        timeout: float,
        retry_strategy: Retry,
        ssl_context: Optional[urllib3.util.ssl_.SSLContext],
        ca_certs: Optional[str],
        cert_reqs: str,
) -> urllib3.PoolManager:
    """Build a reusable ``urllib3.PoolManager`` configured with the given retry strategy, timeout and SSL settings."""
    return urllib3.PoolManager(
        retries=retry_strategy,
        timeout=urllib3.Timeout(total=timeout),
        cert_reqs=cert_reqs,
        ca_certs=ca_certs,
        ssl_context=ssl_context,
    )


def _handle_redirects(
        http: urllib3.PoolManager,
        initial_url: str,
        max_redirects: int,
        allow_private_ip: bool = False,
        validate_host: bool = True,
) -> tuple[urllib3.HTTPResponse, str, int] :
    """Manually follow HTTP redirects up to ``max_redirects`` and return the final
    response, the URL that was finally fetched and the number of redirects
    that were performed.
    """
    current_url = initial_url
    redirect_count = 0

    while True:

        if validate_host:
            host = URL(current_url).hostname
            logger.info(f"Validating URL hostname: {host}...")
            if not _validate_hostname(host, allow_private_ip=allow_private_ip):
                raise ValueError(f"URL '{host}' is not an allowed hostname!")

        try:
            response = http.request(
                "GET",
                current_url,
                redirect=False,  # Do not let urllib3 follow redirects
                preload_content=False,  # Stream the response
            )
        except (MaxRetryError, NewConnectionError, TimeoutError) as exc:
            raise ConnectionError(f"Request to {current_url!r} failed: {exc}") from exc

        # Stop following when we hit a non‑redirect status code
        if not (300 <= response.status < 400):
            return response, current_url, redirect_count

        if redirect_count >= max_redirects:
            raise RuntimeError(
                f"Maximum redirect limit ({max_redirects}) exceeded at {current_url}"
            )

        location = response.headers.get("Location")
        if not location:
            raise RuntimeError(
                f"Redirect response from {current_url!r} has no Location header"
            )

        # Resolve relative redirects
        next_url = join_url(current_url, location)
        if not _parse_url(next_url):
            raise ValueError(f"Redirect target {next_url!r} is not a valid URL")

        current_url = next_url
        redirect_count += 1


@dataclass
class ServerCheckResult:
    """Class containing information about the server's response."""
    url: str
    status_code: int
    ok: bool
    final_url: str
    message: str
    n_redirects: int = 0


def is_server_available(
        url: str,
        timeout: float = 5.0,
        max_redirects: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 0.3,
        allow_private_ip: bool = False,
        validate_host: bool = True,
        cert_path: Optional[str] = None,
        verify_ssl: bool = True,
) -> ServerCheckResult:
    """Check if a server is available by making an HTTP request to the given URL.

    Parameters
    ----------
    url : str
        The URL of the server to check.
    timeout : float, optional
        Maximum number of seconds to wait for a response. Default is ``5.0``.
    max_redirects : int, optional
        Maximum number of redirects to follow. Default is ``2``.
    max_retries : int, optional
        Maximum number of retries in case of failure. Default is ``3``.
    backoff_factor : float, optional
        Multiplier for the delay between retry attempts. Default is ``0.3``.
    allow_private_ip : bool, optional
        If ``True``, private or non‑routable IPs are accepted.
    validate_host : bool, optional
        Whether to validate the hostname against a whitelist/blacklist. Default is ``True``.
    cert_path : str or pathlib.Path, optional
        Path to a CA bundle to use. Default is ``None``.
    verify_ssl : bool, optional
        Whether to verify SSL certificates. Default is ``True``.

    Returns
    -------
    plantdb.client.url.ServerCheckResult
        An object containing details about the server check result.

    Raises
    ------
    ValueError
        If the supplied URL is malformed or a redirect target is invalid.
    RuntimeError
        If the maximum redirect limit is exceeded.
    ConnectionError
        If the HTTP request fails due to network issues.

    Examples
    --------
    >>> from plantdb.client.url import is_server_available
    >>> scr = is_server_available('https://google.com', validate_host=False)
    >>> print(scr.final_url)
    https://www.google.com/
    >>> scr = is_server_available('https://google.com', validate_host=True)
    INFO     [url] Validating URL hostname: google.com...
    INFO     [url] Validating URL hostname: www.google.com...
    print(scr.ok)
    True
    """
    # ------------------------------------------------------------------ #
    # 1. Parse the URL
    # ------------------------------------------------------------------ #
    parsed = _parse_url(url)
    if not parsed:
        message = f"URL '{url}' is not a valid URL."
        logger.warning(message)
        return ServerCheckResult(
            url=url,
            status_code=0,
            ok=False,
            final_url=url,
            message=message,
        )

    # ------------------------------------------------------------------ #
    # 2. Build retry strategy
    # ------------------------------------------------------------------ #
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods={"GET", "HEAD"},
        redirect=max_redirects,
    )

    # ------------------------------------------------------------------ #
    # 3. Configure SSL
    # ------------------------------------------------------------------ #
    cert_path_obj = Path(cert_path) if cert_path is not None else None
    ssl_context, ca_certs, cert_reqs = _build_ssl_context(cert_path_obj, verify_ssl)

    # ------------------------------------------------------------------ #
    # 4. Create HTTP pool manager
    # ------------------------------------------------------------------ #
    http = _configure_http_pool(timeout, retry_strategy, ssl_context, ca_certs, cert_reqs)

    # ------------------------------------------------------------------ #
    # 5. Perform the request and follow redirects manually
    # ------------------------------------------------------------------ #
    try:
        response, final_url, n_redirects = _handle_redirects(http, URL(url).href, max_redirects,
                                                             allow_private_ip, validate_host)
    except (ConnectionError, RuntimeError, ValueError) as exc:
        message = f"Request to {url!r} failed: {exc}"
        logger.error(message)
        return ServerCheckResult(
            url=url,
            status_code=0,
            ok=False,
            final_url=url,
            message=message,
        )

    # ------------------------------------------------------------------ #
    # 6. Build the result object
    # ------------------------------------------------------------------ #
    ok = 200 <= response.status < 400
    return ServerCheckResult(
        url=url,
        status_code=response.status,
        ok=ok,
        final_url=final_url,
        message="Valid URL." if ok else f"HTTP {response.status}",
        n_redirects=n_redirects,
    )
