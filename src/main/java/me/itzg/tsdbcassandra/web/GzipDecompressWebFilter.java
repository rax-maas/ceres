package me.itzg.tsdbcassandra.web;

import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.springframework.boot.web.server.WebServerException;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
                  return serverWebExchange.getRequest().getBody()
                      .map(inBuffer -> {
                        final DataBuffer outBuffer = inBuffer.factory().allocateBuffer();
                        try (GZIPInputStream gis = new GZIPInputStream(
                            inBuffer.asInputStream())) {
                          gis.transferTo(outBuffer.asOutputStream());
                        } catch (IOException e) {
                          throw new WebServerException("Failed to gzip request body", e);
                        }
                        return outBuffer;
                      });
                }
              })
              .build()
      );
    } else {
      return webFilterChain.filter(serverWebExchange);
    }
  }
}
