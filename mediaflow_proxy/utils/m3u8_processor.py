import asyncio
import codecs
import os
import re
from typing import AsyncGenerator
from urllib import parse

from mediaflow_proxy.configs import settings
from mediaflow_proxy.utils.crypto_utils import encryption_handler
from mediaflow_proxy.utils.http_utils import encode_mediaflow_proxy_url, encode_stremio_proxy_url, get_original_scheme
from mediaflow_proxy.utils.hls_prebuffer import hls_prebuffer


class M3U8Processor:
    def __init__(self, request, key_url: str = None, force_playlist_proxy: bool = None, key_only_proxy: bool = False, no_proxy: bool = False):
        """
        Initializes the M3U8Processor with the request and URL prefix.
        """
        self.request = request
        self.key_url = parse.urlparse(key_url) if key_url else None
        self.key_only_proxy = key_only_proxy
        self.no_proxy = no_proxy
        self.force_playlist_proxy = force_playlist_proxy
        self.mediaflow_proxy_url = str(
            request.url_for("hls_manifest_proxy").replace(scheme=get_original_scheme(request))
        )
        # Base per proxy streaming (usata per riscrivere segmenti .css come .ts)
        try:
            self.stream_proxy_base = str(
                request.url_for("proxy_stream_endpoint").replace(scheme=get_original_scheme(request))
            )
        except Exception:
            # fallback: riusa manifest proxy se stream endpoint non disponibile
            self.stream_proxy_base = self.mediaflow_proxy_url

        self.playlist_url = None  # Will be set when processing starts

    async def process_m3u8(self, content: str, base_url: str) -> str:
        """
        Processes the m3u8 content, proxying URLs and handling key lines.
        """
        self.playlist_url = base_url
        lines = content.splitlines()
        processed_lines = []
        for line in lines:
            if "URI=" in line:
                processed_lines.append(await self.process_key_line(line, base_url))
            elif not line.startswith("#") and line.strip():
                processed_lines.append(await self.proxy_content_url(line, base_url))
            else:
                processed_lines.append(line)

        if (settings.enable_hls_prebuffer and 
            "#EXTM3U" in content and
            self.playlist_url):
            headers = {}
            for key, value in self.request.query_params.items():
                if key.startswith("h_"):
                    headers[key[2:]] = value
            asyncio.create_task(
                hls_prebuffer.prebuffer_playlist(self.playlist_url, headers)
            )

        return "\n".join(processed_lines)

    async def process_m3u8_streaming(
        self, content_iterator: AsyncGenerator[bytes, None], base_url: str
    ) -> AsyncGenerator[str, None]:
        """
        Streaming processing of m3u8.
        """
        self.playlist_url = base_url
        buffer = ""
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        is_playlist_detected = False
        is_prebuffer_started = False

        async for chunk in content_iterator:
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            decoded_chunk = decoder.decode(chunk)
            buffer += decoded_chunk

            if not is_playlist_detected and "#EXTM3U" in buffer:
                is_playlist_detected = True

            lines = buffer.split("\n")
            if len(lines) > 1:
                for line in lines[:-1]:
                    if line:
                        processed_line = await self.process_line(line, base_url)
                        yield processed_line + "\n"
                buffer = lines[-1]

            if (settings.enable_hls_prebuffer and 
                is_playlist_detected and 
                not is_prebuffer_started and
                self.playlist_url):
                headers = {}
                for key, value in self.request.query_params.items():
                    if key.startswith("h_"):
                        headers[key[2:]] = value
                asyncio.create_task(
                    hls_prebuffer.prebuffer_playlist(self.playlist_url, headers)
                )
                is_prebuffer_started = True

        final_chunk = decoder.decode(b"", final=True)
        if final_chunk:
            buffer += final_chunk
        if buffer:
            processed_line = await self.process_line(buffer, base_url)
            yield processed_line

    async def process_line(self, line: str, base_url: str) -> str:
        if "URI=" in line:
            return await self.process_key_line(line, base_url)
        elif not line.startswith("#") and line.strip():
            return await self.proxy_content_url(line, base_url)
        else:
            return line

    async def process_key_line(self, line: str, base_url: str) -> str:
        if self.no_proxy:
            uri_match = re.search(r'URI="([^"]+)"', line)
            if uri_match:
                original_uri = uri_match.group(1)
                full_url = parse.urljoin(base_url, original_uri)
                line = line.replace(f'URI="{original_uri}"', f'URI="{full_url}"')
            return line

        uri_match = re.search(r'URI="([^"]+)"', line)
        if uri_match:
            original_uri = uri_match.group(1)
            uri = parse.urlparse(original_uri)
            if self.key_url:
                uri = uri._replace(scheme=self.key_url.scheme, netloc=self.key_url.netloc)
            new_uri = await self.proxy_url(uri.geturl(), base_url)
            line = line.replace(f'URI="{original_uri}"', f'URI="{new_uri}"')
        return line

    async def proxy_content_url(self, url: str, base_url: str) -> str:
        full_url = parse.urljoin(base_url, url)

        if self.no_proxy:
            return full_url

        # Eccezione: se key_only_proxy è attivo ma il segmento ha estensione .css, lo riscriviamo come .ts proxato
        if self.key_only_proxy and not url.endswith((".m3u", ".m3u8")):
            if full_url.split("?")[0].endswith(".css"):
                return await self._rewrite_css_segment(full_url)
            return full_url

        routing_strategy = settings.m3u8_content_routing

        if self.force_playlist_proxy:
            return await self.proxy_url(full_url, base_url, use_full_url=True)

        parsed_url = parse.urlparse(full_url)
        if (parsed_url.path.endswith((".m3u", ".m3u8", ".m3u_plus")) or
            parse.parse_qs(parsed_url.query).get("type", [""])[0] in ["m3u", "m3u8", "m3u_plus"]):
            return await self.proxy_url(full_url, base_url, use_full_url=True)

        if routing_strategy == "direct":
            return full_url
        elif routing_strategy == "stremio" and settings.stremio_proxy_url:
            query_params = dict(self.request.query_params)
            request_headers = {k[2:]: v for k, v in query_params.items() if k.startswith("h_")}
            response_headers = {k[2:]: v for k, v in query_params.items() if k.startswith("r_")}
            return encode_stremio_proxy_url(
                settings.stremio_proxy_url,
                full_url,
                request_headers=request_headers if request_headers else None,
                response_headers=response_headers if response_headers else None,
            )
        else:
            return await self.proxy_url(full_url, base_url, use_full_url=True)

    async def _rewrite_css_segment(self, full_url: str) -> str:
        """
        Riscrive un segmento .css in un URL proxato con estensione .ts (o altra estensione standard).
        Non altera la sorgente originale: il proxy scarica l'URL .css reale.
        """
        parsed = parse.urlparse(full_url)
        original_name = os.path.basename(parsed.path)
        stem = original_name.rsplit(".", 1)[0]
        synthetic_filename = f"{stem}.ts"

        query_params = dict(self.request.query_params)
        has_encrypted = query_params.pop("has_encrypted", False)
        # Rimuove header di risposta e flag non necessari
        [query_params.pop(key, None) for key in list(query_params.keys()) if key.startswith("r_")]
        query_params.pop("force_playlist_proxy", None)

        # Costruisce URL proxato verso endpoint /stream/<filename>
        return encode_mediaflow_proxy_url(
            self.stream_proxy_base,
            synthetic_filename,
            full_url,
            query_params=query_params,
            encryption_handler=encryption_handler if has_encrypted else None,
        )

    async def proxy_url(self, url: str, base_url: str, use_full_url: bool = False) -> str:
        if use_full_url:
            full_url = url
        else:
            full_url = parse.urljoin(base_url, url)

        query_params = dict(self.request.query_params)
        has_encrypted = query_params.pop("has_encrypted", False)
        [query_params.pop(key, None) for key in list(query_params.keys()) if key.startswith("r_")]
        query_params.pop("force_playlist_proxy", None)

        return encode_mediaflow_proxy_url(
            self.mediaflow_proxy_url,
            "",
            full_url,
            query_params=query_params,
            encryption_handler=encryption_handler if has_encrypted else None,
        )
