package me.itzg.tsdbcassandra.web;

import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.springframework.boot.web.server.WebServerException;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This web filter is needed to handle ingest put API calls where the caller gzips the request body,
 * which is the case with telegraf's opentsdb output plugin.
 */
@Component
public class GzipDecompressWebFilter implements WebFilter {

  @Override
  public Mono<Void> filter(ServerWebExchange serverWebExchange, WebFilterChain webFilterChain) {
    final List<String> contentEncoding = serverWebExchange.getRequest().getHeaders()
        .get("Content-Encoding");
    if (contentEncoding != null && contentEncoding.contains("gzip")) {

      return webFilterChain.filter(
          serverWebExchange.mutate()
              .request(new ServerHttpRequestDecorator(serverWebExchange.getRequest()) {
                @Override
                public Flux<DataBuffer> getBody() {
                  return Flux.from(
                      // gzipped body might be split across several DataBuffers from the
                      // request body's flux, so join those into a composite DataBuffer mono.
                      DataBufferUtils.join(serverWebExchange.getRequest().getBody())
                          // ...now decompress the joined DataBuffer
                          .map(joinedBuffer -> {
                            final DataBuffer outBuffer = joinedBuffer.factory().allocateBuffer();
                            try (GZIPInputStream gis = new GZIPInputStream(joinedBuffer.asInputStream())) {
                              gis.transferTo(outBuffer.asOutputStream());
                              return outBuffer;
                            } catch (IOException e) {
                              throw new WebServerException("Failed to gzip request body", e);
                            } finally {
                              // releasing the joined/composite buffer will propagate to the original buffers
                              DataBufferUtils.release(joinedBuffer);
                            }
                          })
                  );
                }
              })
              .build()
      );
    } else {
      return webFilterChain.filter(serverWebExchange);
    }
  }
}
