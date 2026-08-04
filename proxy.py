#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
proxy.py —— 短效代理 -> 本地长效代理
================================================

工作原理:
  1. 定时 / 触发式地从代理 API 获取一批短效代理 IP;
  2. 按配置自动识别返回格式 (txt / json) 并解析出 ip:port 列表;
  3. 测试每个代理的可用性 (可单线程取第一个, 也可多线程测速取最快);
  4. 在本地 127.0.0.1:<端口> 启动代理服务 (http / https / socks5),
     所有流量经由当前选中的上游代理转发;
  5. 心跳包持续检测当前代理活性, 到期或失效时无缝切换到新代理
     (新代理验证通过后才会替换旧代理, 切换过程不中断用户网络)。

仅使用 Python 标准库, 无需安装任何第三方依赖。

用法:
    python proxy.py [config.ini 路径]
"""

import configparser
import json
import os
import socket
import socketserver
import ssl
import struct
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from urllib.request import Request, urlopen

VERSION = "1.0.0"
GEO_API = "https://whois.pconline.com.cn/ipJson.jsp?ip={ip}&json=true"

# ---------------------------------------------------------------------------
# 多语言文案
# ---------------------------------------------------------------------------
MSG = {
    "cn": {
        "startup": "代理轮换服务已启动 | 本地监听 127.0.0.1:{port} | 输出协议: {proto}",
        "loading_cfg": "加载配置文件: {path}",
        "cfg_not_found": "配置文件不存在, 使用内置默认配置: {path}",
        "fetching": "正在从 API 获取代理列表 ...",
        "fetched": "API 返回 {n} 个代理候选",
        "fmt_json": "检测到 API 返回格式: JSON",
        "fmt_txt": "检测到 API 返回格式: TXT",
        "err_json_no_path": "API 返回了 JSON, 但配置文件中未设置 json_ip_paths, 无法解析 (默认只支持 txt)",
        "err_no_proxy": "未能从 API 返回内容中解析出任何有效代理",
        "testing": "开始测试代理可用性 (多线程={mt}) ...",
        "test_ok": "代理 {ip}:{port} [{proto}] 可用, 延迟 {lat:.2f}s",
        "test_fail": "代理 {ip}:{port} [{proto}] 不可用",
        "no_usable": "本轮所有候选代理均不可用",
        "fastest": "多线程测速完成, 选用最快代理 {ip}:{port} [{proto}] 延迟 {lat:.2f}s",
        "first_ok": "单线程模式, 选用第一个可用代理 {ip}:{port} [{proto}]",
        "switched": "已切换到新代理: {ip}:{port} [{proto}]",
        "geo": "当前代理地理位置: {addr} ",
        "geo_fail": "地理位置查询失败: {err}",
        "hb_ok": "心跳正常 (延迟 {lat:.2f}s)",
        "hb_dead": "心跳检测失败, 当前代理已失效, 立即更换",
        "hb_force": "已达到强制更换周期 ({n}s), 主动更换代理",
        "hb_unavail": "代理已被标记为不可用, 立即更换",
        "consec_fail": "连续请求失败 {n} 次, 立即更换代理",
        "standby_fetched": "已预取备胎代理 {ip}:{port} [{proto}] ({cur}/{total})",
        "rotate_retry": "获取可用代理失败 (第 {n} 次), {wait:.0f}s 后重试: {err}",
        "fatal": "已连续失败 {n} 次, 达到最大尝试次数, 程序退出。请检查 API 与网络后重新启动。",
        "rotate_none": "当前无可用代理, 正在获取 ...",
        "proto_auto": "自动匹配协议: {ip}:{port} -> {proto}",
        "banner_out": "请在其他软件中填写代理地址 127.0.0.1:{port} 即可使用",
        "shutdown": "收到退出信号, 正在关闭 ...",
        "bad_separator": "自定义分隔符不能为 '.' 或 ':', 已忽略, 使用默认按行分割",
        "listen_fail": "本地端口 {port} 监听失败: {err}",
    },
    "en": {
        "startup": "Proxy rotator started | listening on 127.0.0.1:{port} | output protocol: {proto}",
        "loading_cfg": "Loading config file: {path}",
        "cfg_not_found": "Config file not found, using built-in defaults: {path}",
        "fetching": "Fetching proxy list from API ...",
        "fetched": "API returned {n} proxy candidates",
        "fmt_json": "Detected API response format: JSON",
        "fmt_txt": "Detected API response format: TXT",
        "err_json_no_path": "API returned JSON but json_ip_paths is not set in config (only txt is supported by default)",
        "err_no_proxy": "No valid proxy could be parsed from the API response",
        "testing": "Testing proxy availability (multithread={mt}) ...",
        "test_ok": "Proxy {ip}:{port} [{proto}] OK, latency {lat:.2f}s",
        "test_fail": "Proxy {ip}:{port} [{proto}] FAILED",
        "no_usable": "All candidates in this round are unusable",
        "fastest": "Speed test done, picked fastest proxy {ip}:{port} [{proto}] latency {lat:.2f}s",
        "first_ok": "Single-thread mode, picked first working proxy {ip}:{port} [{proto}]",
        "switched": "Switched to new proxy: {ip}:{port} [{proto}]",
        "geo": "Current proxy location: {addr} ",
        "geo_fail": "Geo lookup failed: {err}",
        "hb_ok": "Heartbeat OK (latency {lat:.2f}s)",
        "hb_dead": "Heartbeat failed, current proxy is dead, rotating now",
        "hb_force": "Force-rotate interval reached ({n}s), rotating proxy",
        "hb_unavail": "Proxy marked unavailable, rotating now",
        "consec_fail": "Consecutive request failures reached {n}, rotating proxy",
        "standby_fetched": "Prefetched standby proxy {ip}:{port} [{proto}] ({cur}/{total})",
        "rotate_retry": "Failed to acquire a working proxy (attempt {n}), retry in {wait:.0f}s: {err}",
        "fatal": "Failed {n} times in a row (max retries reached), exiting. Check API and network.",
        "rotate_none": "No active proxy, acquiring one ...",
        "proto_auto": "Auto-detected protocol: {ip}:{port} -> {proto}",
        "banner_out": "Set 127.0.0.1:{port} as proxy in your other software to use it",
        "shutdown": "Shutdown signal received, closing ...",
        "bad_separator": "Custom separator cannot be '.' or ':', ignored; using line-based split",
        "listen_fail": "Failed to listen on local port {port}: {err}",
    },
}

LANG = "cn"


def L(key, **kw):
    tpl = MSG.get(LANG, MSG["cn"]).get(key, key)
    try:
        return tpl.format(**kw)
    except Exception:
        return tpl


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
DEFAULTS = {
    "api_url": "",
    "input_protocol": "auto",        # auto / http / https / socks5
    "output_protocol": "same",       # same / http / https / socks5
    "force_change_interval": "60",   # 秒, 到期强制更换 IP
    "txt_separator": "",             # txt 分隔符, 留空 = 一行一个
    "json_ip_paths": "",             # json ip 路径, 分号隔开多个
    "json_port_path": "",            # json 端口路径 (可选)
    "test_url": "https://whois.pconline.com.cn/ipJson.jsp?ip=&json=true",
    "timeout": "3",
    "heartbeat_interval": "10",
    "multithread_test": "false",
    "show_location": "true",
    "language": "cn",
    "listen_port": "1217",
    "max_retries": "10",             # 0 = 无限重试
    "standby_pool_size": "2",        # 预备 IP 数量, 0 = 禁用
}


class Config:
    def __init__(self, path):
        parser = configparser.ConfigParser()
        if os.path.isfile(path):
            parser.read(path, encoding="utf-8")
            log(L("loading_cfg", path=path))
        else:
            log(L("cfg_not_found", path=path))
        sec = parser["proxy"] if parser.has_section("proxy") else {}

        def get(key):
            v = sec.get(key, DEFAULTS[key])
            return v.strip() if isinstance(v, str) else v

        self.api_url = get("api_url")
        self.input_protocol = get("input_protocol").lower()
        self.output_protocol = get("output_protocol").lower()
        self.force_change_interval = self._f(get("force_change_interval"), 60.0)
        self.txt_separator = get("txt_separator")
        if self.txt_separator in (".", ":"):
            log(L("bad_separator"))
            self.txt_separator = ""
        self.json_ip_paths = get("json_ip_paths")
        self.json_port_path = get("json_port_path")
        self.test_url = get("test_url")
        self.timeout = self._f(get("timeout"), 3.0)
        self.heartbeat_interval = self._f(get("heartbeat_interval"), 10.0)
        self.multithread_test = get("multithread_test").lower() in ("1", "true", "yes")
        self.show_location = get("show_location").lower() in ("1", "true", "yes")
        self.language = get("language").lower()
        self.listen_port = self._i(get("listen_port"), 1217)
        self.max_retries = self._i(get("max_retries"), 10)
        self.standby_pool_size = max(0, self._i(get("standby_pool_size"), 2))

    @staticmethod
    def _f(v, d):
        try:
            return float(v)
        except (TypeError, ValueError):
            return d

    @staticmethod
    def _i(v, d):
        try:
            return int(v)
        except (TypeError, ValueError):
            return d


# ---------------------------------------------------------------------------
# 网络基础工具
# ---------------------------------------------------------------------------
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed")
        buf += chunk
    return buf


def read_header_block(sock, cap=65536):
    """读取直到 HTTP 头结束 (\\r\\n\\r\\n), 返回已读字节."""
    buf = b""
    while b"\r\n\r\n" not in buf and len(buf) < cap:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
    return buf


def parse_status_code(header_block):
    try:
        first = header_block.split(b"\r\n", 1)[0].decode("latin1")
        return int(first.split(" ")[1])
    except Exception:
        return None


def http_connect(sock, host, port):
    """通过 HTTP 代理建立 CONNECT 隧道, 成功返回 True."""
    req = ("CONNECT %s:%d HTTP/1.1\r\nHost: %s:%d\r\n"
           "User-Agent: %s\r\n\r\n" % (host, port, host, port, UA))
    sock.sendall(req.encode("latin1"))
    buf = read_header_block(sock)
    return parse_status_code(buf) == 200


def socks5_handshake(sock, host, port):
    """通过 SOCKS5 代理建立到 host:port 的连接, 失败抛异常."""
    sock.sendall(b"\x05\x01\x00")
    resp = recv_exact(sock, 2)
    if resp != b"\x05\x00":
        raise ConnectionError("socks5 auth negotiation failed")
    try:
        packed = socket.inet_aton(host)
        addr = b"\x01" + packed
    except OSError:
        hb = host.encode("idna")
        if len(hb) > 255:
            raise ConnectionError("host name too long")
        addr = b"\x03" + bytes([len(hb)]) + hb
    sock.sendall(b"\x05\x01\x00" + addr + struct.pack("!H", port))
    head = recv_exact(sock, 4)
    if head[1] != 0x00:
        raise ConnectionError("socks5 connect refused, rep=%d" % head[1])
    atyp = head[3]
    if atyp == 0x01:
        recv_exact(sock, 4)
    elif atyp == 0x03:
        recv_exact(sock, recv_exact(sock, 1)[0])
    elif atyp == 0x04:
        recv_exact(sock, 16)
    recv_exact(sock, 2)


def send_get(sock, host, path):
    req = ("GET %s HTTP/1.1\r\nHost: %s\r\nUser-Agent: %s\r\n"
           "Accept: */*\r\nConnection: close\r\n\r\n" % (path, host, UA))
    sock.sendall(req.encode("latin1"))


def fetch_raw(url, timeout):
    req = Request(url, headers={"User-Agent": UA})
    with urlopen(req, timeout=timeout) as r:
        raw = r.read()
    for enc in ("utf-8", "gbk"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", "ignore")


# ---------------------------------------------------------------------------
# API 返回解析 (txt / json 自动识别)
# ---------------------------------------------------------------------------
def resolve_path(data, path):
    """解析形如 data[*].ip / proxy.ip / ip 的路径, 返回值的列表."""
    results = [data]
    for token in path.split("."):
        token = token.strip()
        if not token:
            continue
        is_list = token.endswith("[*]")
        key = token[:-3] if is_list else token
        nxt = []
        for item in results:
            if key:
                if isinstance(item, dict) and key in item:
                    val = item[key]
                else:
                    continue
            else:
                val = item
            if is_list:
                if isinstance(val, list):
                    nxt.extend(val)
            else:
                nxt.append(val)
        results = nxt
    return results


def split_ip_port(text):
    text = text.strip()
    if ":" not in text:
        return None
    host, _, port = text.rpartition(":")
    try:
        return host.strip(), int(port.strip())
    except ValueError:
        return None


def parse_proxies(text, cfg):
    """自动识别 txt / json 并按配置解析, 返回 [(ip, port), ...]."""
    text = text.strip()
    data = None
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        pass

    if data is not None:
        log(L("fmt_json"))
        if not cfg.json_ip_paths:
            raise ValueError(L("err_json_no_path"))
        ips = []
        for p in cfg.json_ip_paths.split(";"):
            p = p.strip()
            if p:
                ips.extend(resolve_path(data, p))
        ports = resolve_path(data, cfg.json_port_path) if cfg.json_port_path else []
        result = []
        for i, ip in enumerate(ips):
            ip = str(ip).strip()
            if not ip:
                continue
            if ":" in ip:
                pair = split_ip_port(ip)
                if pair:
                    result.append(pair)
            elif i < len(ports):
                try:
                    result.append((ip, int(str(ports[i]).strip())))
                except ValueError:
                    log("port parse failed for %s" % ip)
        if not result:
            raise ValueError(L("err_no_proxy"))
        return result

    # txt 模式
    log(L("fmt_txt"))
    parts = text.split(cfg.txt_separator) if cfg.txt_separator else text.splitlines()
    result = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        pair = split_ip_port(part)
        if pair:
            result.append(pair)
    if not result:
        raise ValueError(L("err_no_proxy"))
    return result


# ---------------------------------------------------------------------------
# 代理可用性测试
# ---------------------------------------------------------------------------
def try_request_via_proxy(proto, ip, port, test_url, timeout):
    """通过指定协议经代理请求 test_url, 成功返回延迟(秒), 失败返回 None."""
    p = urlparse(test_url)
    host = p.hostname
    if not host:
        return None
    tport = p.port or (443 if p.scheme == "https" else 80)
    path = p.path or "/"
    if p.query:
        path += "?" + p.query

    start = time.time()
    sock = None
    try:
        sock = socket.create_connection((ip, port), timeout=timeout)
        sock.settimeout(timeout)

        if proto in ("http", "https"):
            if proto == "https":  # 代理本身走 TLS
                sock = SSL_CTX.wrap_socket(sock, server_hostname=ip)
                sock.settimeout(timeout)
            if p.scheme == "https":
                if not http_connect(sock, host, tport):
                    return None
                tls = SSL_CTX.wrap_socket(sock, server_hostname=host)
                tls.settimeout(timeout)
                send_get(tls, host, path)
                code = parse_status_code(read_header_block(tls))
                tls.close()
            else:
                # 明文 http 直接发绝对 URI 给代理
                send_get(sock, host, test_url)
                code = parse_status_code(read_header_block(sock))
        else:  # socks5
            socks5_handshake(sock, host, tport)
            if p.scheme == "https":
                tls = SSL_CTX.wrap_socket(sock, server_hostname=host)
                tls.settimeout(timeout)
                send_get(tls, host, path)
                code = parse_status_code(read_header_block(tls))
                tls.close()
            else:
                send_get(sock, host, path)
                code = parse_status_code(read_header_block(sock))

        if code is not None and 200 <= code < 400:
            return time.time() - start
        log("status code %s from %s:%s [%s]" % (code, ip, port, proto))
        return None
    except Exception as e:
        log("test error %s:%s [%s]: %s" % (ip, port, proto, e))
        return None
    finally:
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass


def test_proxy(ip, port, cfg):
    """按配置测试代理, 返回 (protocol, latency) 或 None. 自动模式优先 socks5."""
    if cfg.input_protocol == "auto":
        protos = ["socks5", "http", "https"]
    else:
        protos = [cfg.input_protocol]
    for proto in protos:
        lat = try_request_via_proxy(proto, ip, port, cfg.test_url, cfg.timeout)
        if lat is not None:
            if cfg.input_protocol == "auto":
                log(L("proto_auto", ip=ip, port=port, proto=proto))
            log(L("test_ok", ip=ip, port=port, proto=proto, lat=lat))
            return proto, lat
        log(L("test_fail", ip=ip, port=port, proto=proto))
    return None


# ---------------------------------------------------------------------------
# 轮换管理器
# ---------------------------------------------------------------------------
class Rotator:
    def __init__(self, cfg):
        self.cfg = cfg
        self.current = None              # {"ip","port","protocol","since","latency","available"}
        self.lock = threading.Lock()     # 保护 current 的替换
        self.rotate_lock = threading.Lock()
        self.acquire_lock = threading.Lock()
        self.standby_lock = threading.Lock()
        self.standby = []                # 已获取并验证的待用代理
        self.fail_count = 0
        self.stop_event = threading.Event()
        self.rotate_event = threading.Event()  # 立即触发轮换的信号
        self.consecutive_failures = 0    # 连续请求失败计数
        self.request_lock = threading.Lock()  # 保护失败计数

    def get_current(self):
        with self.lock:
            return dict(self.current) if self.current else None

    def current_is_available(self):
        with self.lock:
            return bool(self.current and self.current.get("available", True))

    def mark_current_unavailable(self, expected=None):
        with self.lock:
            if self.current and (expected is None or
                                  self.current.get("ip") == expected.get("ip") and
                                  self.current.get("port") == expected.get("port") and
                                  self.current.get("protocol") == expected.get("protocol")):
                self.current["available"] = False

    def record_request_failure(self):
        """记录一次请求失败, 连续失败3次时触发立即轮换."""
        with self.request_lock:
            self.consecutive_failures += 1
            if self.consecutive_failures >= 3:
                log(L("consec_fail", n=self.consecutive_failures))
                self.mark_current_unavailable()
                self.rotate_event.set()  # 唤醒 maintain_loop 立即轮换

    def record_request_success(self):
        """记录一次请求成功, 重置失败计数."""
        with self.request_lock:
            self.consecutive_failures = 0

    def reset_failure_counter(self):
        """轮换成功后重置连续失败计数."""
        with self.request_lock:
            self.consecutive_failures = 0

    def discard_expired_standby(self):
        now = time.time()
        with self.standby_lock:
            self.standby = [p for p in self.standby
                            if now - p["since"] < self.cfg.force_change_interval]

    def take_standby(self):
        self.discard_expired_standby()
        with self.standby_lock:
            if self.standby:
                return self.standby.pop(0)
        return None

    def add_standby(self, proxy):
        with self.standby_lock:
            if len(self.standby) >= self.cfg.standby_pool_size:
                return False
            if any(p["ip"] == proxy["ip"] and p["port"] == proxy["port"] and
                   p["protocol"] == proxy["protocol"] for p in self.standby):
                return False
            self.standby.append(proxy)
            return True

    def standby_count(self):
        self.discard_expired_standby()
        with self.standby_lock:
            return len(self.standby)

    # -- 获取并挑选一个可用代理 --
    def acquire(self):
        with self.acquire_lock:
            return self._acquire()

    def _acquire(self):
        cfg = self.cfg
        log(L("fetching"))
        raw = fetch_raw(cfg.api_url, cfg.timeout + 2)
        candidates = parse_proxies(raw, cfg)
        acquired_at = time.time()
        log(L("fetched", n=len(candidates)))
        log(L("testing", mt=cfg.multithread_test))

        if cfg.multithread_test:
            pool = candidates[:10]  # 最多测试 10 个
            results = []
            with ThreadPoolExecutor(max_workers=len(pool) or 1) as ex:
                futs = {ex.submit(test_proxy, ip, port, cfg): (ip, port)
                        for ip, port in pool}
                for fut in as_completed(futs):
                    ip, port = futs[fut]
                    r = fut.result()
                    if r:
                        results.append((r[1], r[0], ip, port))
            if not results:
                raise RuntimeError(L("no_usable"))
            results.sort(key=lambda x: x[0])
            lat, proto, ip, port = results[0]
            log(L("fastest", ip=ip, port=port, proto=proto, lat=lat))
        else:
            picked = None
            for ip, port in candidates:
                r = test_proxy(ip, port, cfg)
                if r:
                    picked = (ip, port, r[0], r[1])
                    break
            if not picked:
                raise RuntimeError(L("no_usable"))
            ip, port, proto, lat = picked
            log(L("first_ok", ip=ip, port=port, proto=proto))

        return {"ip": ip, "port": port, "protocol": proto,
                "since": acquired_at, "latency": lat, "available": True}

    # -- 轮换 (新代理验证通过后才替换, 保证切换丝滑) --
    def rotate(self):
        with self.rotate_lock:
            chosen = self.take_standby() if self.cfg.standby_pool_size else None
            if chosen is None:
                try:
                    chosen = self.acquire()
                except Exception as e:
                    self.fail_count += 1
                    return False, str(e)
            self.fail_count = 0
            self.reset_failure_counter()  # 轮换成功后重置连续失败计数
            with self.lock:
                self.current = chosen
            log(L("switched", ip=chosen["ip"], port=chosen["port"],
                  proto=chosen["protocol"]))
            if self.cfg.show_location:
                self.show_geo(chosen["ip"])
            return True, ""

    def standby_loop(self):
        cfg = self.cfg
        if cfg.standby_pool_size <= 0:
            return
        while not self.stop_event.is_set():
            if not self.current_is_available():
                self.stop_event.wait(cfg.heartbeat_interval)
                continue

            cur = self.get_current()
            if not cur:
                self.stop_event.wait(cfg.heartbeat_interval)
                continue

            # 计算当前应该预取的备胎数量
            # N 个备胎将有效期分成 N+1 份, 每过 1/(N+1) 就应该有一个新备胎
            elapsed = time.time() - cur["since"]
            interval = cfg.force_change_interval
            pool_size = cfg.standby_pool_size

            # 应该拥有的备胎数 = floor(elapsed / (interval / (pool_size + 1)))
            expected_count = min(pool_size, int(elapsed / (interval / (pool_size + 1))))
            current_count = self.standby_count()

            if current_count >= expected_count:
                # 还不到下一个备胎的预取时机, 等待一小段时间
                self.stop_event.wait(min(cfg.heartbeat_interval, 1.0))
                continue

            # 需要预取新的备胎
            try:
                candidate = self.acquire()
                # 确保不是当前正在使用的代理
                if (candidate["ip"] == cur["ip"] and
                        candidate["port"] == cur["port"] and
                        candidate["protocol"] == cur["protocol"]):
                    self.stop_event.wait(cfg.heartbeat_interval)
                    continue
                added = self.add_standby(candidate)
                if added:
                    log(L("standby_fetched", ip=candidate["ip"], port=candidate["port"],
                          proto=candidate["protocol"], cur=self.standby_count(), total=pool_size))
            except Exception as e:
                log("standby acquire failed: %s" % e)
                self.stop_event.wait(cfg.heartbeat_interval)

    def show_geo(self, ip):
        try:
            text = fetch_raw(GEO_API.format(ip=ip), self.cfg.timeout + 2)
            addr = json.loads(text).get("addr", "") or ip
            log(L("geo", addr=addr))
        except Exception as e:
            log(L("geo_fail", err=e))

    def backoff(self):
        return min(2 ** max(0, self.fail_count - 1), 10)

    def max_retries_exceeded(self):
        return self.cfg.max_retries > 0 and self.fail_count >= self.cfg.max_retries

    # -- 心跳 + 强制轮换主循环 --
    def maintain_loop(self, on_fatal):
        cfg = self.cfg
        while not self.stop_event.is_set():
            # 检查是否有立即轮换的信号 (连续失败触发)
            if self.rotate_event.is_set():
                self.rotate_event.clear()
                cur = self.get_current()
                if cur and not cur.get("available", True):
                    log(L("hb_unavail"))
                    need_rotate = True
                else:
                    need_rotate = False
            else:
                cur = self.get_current()
                need_rotate = False
                if cur is None:
                    log(L("rotate_none"))
                    need_rotate = True
                elif time.time() - cur["since"] >= cfg.force_change_interval:
                    log(L("hb_force", n=int(cfg.force_change_interval)))
                    need_rotate = True
                else:
                    lat = try_request_via_proxy(cur["protocol"], cur["ip"], cur["port"],
                                                cfg.test_url, cfg.timeout)
                    if lat is None:
                        self.mark_current_unavailable(cur)
                        log(L("hb_dead"))
                        need_rotate = True
                    else:
                        log(L("hb_ok", lat=lat))
                        with self.lock:
                            if self.current:
                                self.current["latency"] = lat

            if need_rotate:
                # 失败则按指数退避持续重试, 直到成功或达到最大次数
                while not self.stop_event.is_set():
                    ok, err = self.rotate()
                    if ok:
                        break
                    if self.max_retries_exceeded():
                        log(L("fatal", n=self.fail_count))
                        on_fatal()
                        return
                    wait = self.backoff()
                    log(L("rotate_retry", n=self.fail_count, wait=wait, err=err))
                    self.stop_event.wait(wait)

            # 等待心跳间隔或立即轮换信号
            self.rotate_event.wait(cfg.heartbeat_interval)


# ---------------------------------------------------------------------------
# 本地代理服务 (HTTP + SOCKS5 自动识别)
# ---------------------------------------------------------------------------
def open_tunnel_via_upstream(up, host, port, timeout):
    """经当前上游代理建立到 host:port 的隧道 socket."""
    proto = up["protocol"]
    sock = socket.create_connection((up["ip"], up["port"]), timeout=timeout)
    sock.settimeout(None)
    try:
        if proto == "https":
            sock = SSL_CTX.wrap_socket(sock, server_hostname=up["ip"])
        if proto in ("http", "https"):
            if not http_connect(sock, host, port):
                raise ConnectionError("upstream CONNECT refused")
        else:
            sock.settimeout(timeout)
            socks5_handshake(sock, host, port)
            sock.settimeout(None)
        return sock
    except Exception:
        try:
            sock.close()
        except OSError:
            pass
        raise


def pipe_bidirectional(a, b):
    """双向转发, 任一端断开则两端一起关闭."""
    for sock in (a, b):
        sock.settimeout(30)

    def relay(src, dst):
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            try:
                dst.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    t1 = threading.Thread(target=relay, args=(a, b), daemon=True)
    t2 = threading.Thread(target=relay, args=(b, a), daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    for s in (a, b):
        try:
            s.close()
        except OSError:
            pass


class ProxyHandler(socketserver.BaseRequestHandler):
    timeout = 30

    def handle(self):
        client = self.request
        try:
            client.settimeout(self.timeout)
            first = client.recv(1, socket.MSG_PEEK)
            if not first:
                return
            if first[0] == 0x05:
                self.handle_socks5(client)
            else:
                self.handle_http(client)
        except (OSError, ConnectionError) as e:
            log("connection error: %s" % e)
        except Exception as e:
            log("unexpected handler error: %s" % e)

    # ---- SOCKS5 服务端 ----
    def handle_socks5(self, client):
        head = recv_exact(client, 2)
        if head[0] != 0x05:
            return
        nmethods = head[1]
        recv_exact(client, nmethods)
        client.sendall(b"\x05\x00")  # 无需认证

        req = recv_exact(client, 4)
        if req[1] != 0x01:  # 仅支持 CONNECT
            client.sendall(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
            return
        atyp = req[3]
        if atyp == 0x01:
            host = socket.inet_ntoa(recv_exact(client, 4))
        elif atyp == 0x03:
            host = recv_exact(client, recv_exact(client, 1)[0]).decode("idna")
        elif atyp == 0x04:
            host = socket.inet_ntop(socket.AF_INET6, recv_exact(client, 16))
        else:
            return
        port = struct.unpack("!H", recv_exact(client, 2))[0]

        up = ROTATOR.get_current()
        if up is None or not up.get("available", True):
            client.sendall(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00")
            return
        log("socks5 request -> %s:%d via %s:%s" % (host, port, up["ip"], up["port"]))
        try:
            upstream = open_tunnel_via_upstream(up, host, port, ROTATOR.cfg.timeout + 2)
            ROTATOR.record_request_success()  # 记录成功
        except Exception as e:
            ROTATOR.record_request_failure()  # 记录失败
            client.sendall(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00")
            log("socks5 tunnel failed: %s" % e)
            return
        client.sendall(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
        client.settimeout(None)
        pipe_bidirectional(client, upstream)

    # ---- HTTP 代理服务端 ----
    def handle_http(self, client):
        buf = read_header_block(client)
        if not buf:
            return
        try:
            head_part, _, rest = buf.partition(b"\r\n\r\n")
            lines = head_part.split(b"\r\n")
            method, target, version = lines[0].decode("latin1").split(" ", 2)
        except ValueError:
            return

        up = ROTATOR.get_current()
        if up is None or not up.get("available", True):
            client.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            return

        if method.upper() == "CONNECT":
            host, _, port = target.partition(":")
            port = int(port or 443)
            log("http CONNECT -> %s:%d via %s:%s" % (host, port, up["ip"], up["port"]))
            try:
                upstream = open_tunnel_via_upstream(up, host, port,
                                                    ROTATOR.cfg.timeout + 2)
                ROTATOR.record_request_success()  # 记录成功
            except Exception as e:
                ROTATOR.record_request_failure()  # 记录失败
                client.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                log("http CONNECT tunnel failed: %s" % e)
                return
            client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            if rest:
                upstream.sendall(rest)
            client.settimeout(None)
            pipe_bidirectional(client, upstream)
            return

        # 普通 http 请求: 解析目标 host
        host, port = self._target_host(target, lines)
        if not host:
            client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return
        log("http %s -> %s:%d via %s:%s" % (method, host, port, up["ip"], up["port"]))

        try:
            if up["protocol"] in ("http", "https"):
                # 上游是 HTTP 代理: 原样转发绝对 URI 请求
                upstream = socket.create_connection((up["ip"], up["port"]),
                                                    timeout=ROTATOR.cfg.timeout + 2)
                if up["protocol"] == "https":
                    upstream = SSL_CTX.wrap_socket(upstream, server_hostname=up["ip"])
                upstream.sendall(buf)
            else:
                # 上游是 SOCKS5: 建隧道后需把请求行改回 origin-form
                upstream = open_tunnel_via_upstream(up, host, port,
                                                    ROTATOR.cfg.timeout + 2)
                path = target
                if target.startswith("http://") or target.startswith("https://"):
                    pu = urlparse(target)
                    path = pu.path or "/"
                    if pu.query:
                        path += "?" + pu.query
                new_head = ("%s %s %s" % (method, path, version)).encode("latin1")
                upstream.sendall(new_head + b"\r\n" +
                                 b"\r\n".join(lines[1:]) + b"\r\n\r\n" + rest)
            ROTATOR.record_request_success()  # 记录成功
        except Exception as e:
            ROTATOR.record_request_failure()  # 记录失败
            client.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            log("http request failed: %s" % e)
            return
        upstream.settimeout(None)
        client.settimeout(None)
        pipe_bidirectional(client, upstream)

    @staticmethod
    def _target_host(target, lines):
        if target.startswith("http://") or target.startswith("https://"):
            pu = urlparse(target)
            return pu.hostname, pu.port or 80
        for line in lines[1:]:
            if line.lower().startswith(b"host:"):
                v = line[5:].decode("latin1").strip()
                host, _, port = v.partition(":")
                return host, int(port) if port else 80
        return None, None


class ThreadedServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
ROTATOR = None  # 全局, 供 handler 使用


def main():
    global LANG, ROTATOR

    cfg_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "config.ini")
    cfg = Config(cfg_path)
    LANG = cfg.language if cfg.language in MSG else "cn"

    if not cfg.api_url:
        log("ERROR: api_url is empty / api_url 未配置, 请编辑 config.ini")
        sys.exit(1)

    ROTATOR = Rotator(cfg)

    def on_fatal():
        os._exit(2)

    # 启动前先拿到第一个可用代理
    while True:
        ok, err = ROTATOR.rotate()
        if ok:
            break
        if ROTATOR.max_retries_exceeded():
            log(L("fatal", n=ROTATOR.fail_count))
            sys.exit(2)
        wait = ROTATOR.backoff()
        log(L("rotate_retry", n=ROTATOR.fail_count, wait=wait, err=err))
        time.sleep(wait)

    # 心跳 / 强制轮换线程
    t = threading.Thread(target=ROTATOR.maintain_loop, args=(on_fatal,), daemon=True)
    t.start()

    # 后台预取并补充待用代理
    standby_thread = threading.Thread(target=ROTATOR.standby_loop, daemon=True)
    standby_thread.start()

    # 本地代理服务
    out_proto = cfg.output_protocol if cfg.output_protocol != "same" else \
        (ROTATOR.get_current() or {}).get("protocol", "http")
    try:
        server = ThreadedServer(("127.0.0.1", cfg.listen_port), ProxyHandler)
    except OSError as e:
        log(L("listen_fail", port=cfg.listen_port, err=e))
        sys.exit(1)

    log(L("startup", port=cfg.listen_port, proto=out_proto))
    log(L("banner_out", port=cfg.listen_port))

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log(L("shutdown"))
    finally:
        ROTATOR.stop_event.set()
        server.server_close()


if __name__ == "__main__":
    main()
