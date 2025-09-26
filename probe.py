#!/usr/bin/env python3
"""
probe_http.py - probe HTTP(S) accessibility for a given URL.

Works with Python 2.7+ and all Python 3.x versions.

Usage:
    python probe_http.py https://example.com
    python probe_http.py https://example.com --method GET --timeout 3 --no-verify
"""
import sys
import time
import argparse
import json
import socket

def probe_with_requests(url, method="HEAD", timeout=5.0,
                        verify=True, retries=1, allow_redirects=True):
    try:
        import requests
        from requests.adapters import HTTPAdapter
        try:
            # urllib3 Retry import changed across versions
            from urllib3.util.retry import Retry
        except ImportError:
            Retry = None
    except Exception:
        raise RuntimeError("requests not available")

    s = requests.Session()
    if Retry is not None:
        retry = Retry(total=retries, backoff_factor=0.5,
                      status_forcelist=(429, 500, 502, 503, 504),
                      method_whitelist=frozenset(["HEAD", "GET", "OPTIONS"]))
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://", HTTPAdapter(max_retries=retry))

    start = time.time()
    try:
        resp = s.request(method, url, timeout=timeout,
                         verify=verify, allow_redirects=allow_redirects)
        elapsed = time.time() - start
        return {
            "ok": True,
            "status_code": resp.status_code,
            "reason": resp.reason,
            "url": resp.url,
            "elapsed": elapsed,
            "headers": dict(resp.headers),
        }
    except Exception as e:
        return {"ok": False, "error": str(type(e)), "detail": str(e)}

def probe_with_urllib(url, method="HEAD", timeout=5.0,
                      verify=True, allow_redirects=True):
    import ssl
    try:
        import urllib2 as urllib_request   # Python 2
    except ImportError:
        import urllib.request as urllib_request  # Python 3
        import urllib.error as urllib_error
    else:
        urllib_error = urllib_request

    start = time.time()
    req = urllib_request.Request(url)
    if method != "GET":
        req.get_method = lambda: method

    ctx = None
    if not verify and url.lower().startswith("https"):
        try:
            ctx = ssl._create_unverified_context()
        except AttributeError:
            ctx = None  # older Python may not support this

    try:
        if ctx:
            resp = urllib_request.urlopen(req, timeout=timeout, context=ctx)
        else:
            resp = urllib_request.urlopen(req, timeout=timeout)
        elapsed = time.time() - start
        return {
            "ok": True,
            "status_code": getattr(resp, "code", None),
            "reason": getattr(resp, "reason", None),
            "url": resp.geturl(),
            "elapsed": elapsed,
            "headers": dict(resp.info().items()),
        }
    except urllib_error.HTTPError as e:
        return {"ok": False, "error": "http_error", "status_code": e.code, "detail": str(e)}
    except urllib_error.URLError as e:
        return {"ok": False, "error": "url_error", "detail": str(e)}
    except socket.timeout:
        return {"ok": False, "error": "timeout"}
    except Exception as e:
        return {"ok": False, "error": str(type(e)), "detail": str(e)}

def probe(url, method="HEAD", timeout=5.0,
          verify=True, retries=1, allow_redirects=True):
    try:
        return probe_with_requests(url, method, timeout, verify, retries, allow_redirects)
    except RuntimeError:
        return probe_with_urllib(url, method, timeout, verify, allow_redirects)

def main(argv=None):
    p = argparse.ArgumentParser(description="Probe HTTP(S) accessibility.")
    p.add_argument("url", help="URL to probe (include http:// or https://)")
    p.add_argument("--method", choices=["HEAD", "GET"], default="HEAD", help="HTTP method to use")
    p.add_argument("--timeout", type=float, default=5.0, help="seconds before timeout")
    p.add_argument("--retries", type=int, default=1, help="number of retries (requests only)")
    p.add_argument("--no-verify", action="store_true", help="do not verify TLS certificates (insecure)")
    args = p.parse_args(argv)

    result = probe(
        args.url,
        method=args.method,
        timeout=args.timeout,
        verify=not args.no_verify,
        retries=args.retries,
        allow_redirects=True,
    )
    if result['ok']:
        with open("http", "w") as f:
            f.write("ok")
    else:
        print("Connectivity check failed")
        print(result)
        time.sleep(1)
        

if __name__ == "__main__":
    main()