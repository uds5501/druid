package org.apache.druid.guice.http;

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.eclipse.jetty.client.api.Request;

import java.util.logging.Logger;

public class CustomRequestListener implements Request.Listener {

  private final String annotationName;
  private static final EmittingLogger LOG = new EmittingLogger(CustomRequestListener.class);

  public CustomRequestListener(String annotationName) {
    this.annotationName = annotationName;
  }

  @Override
  public void onBegin(Request request) {
    // Custom logic to execute when a request begins
    LOG.info("[%s] Request received for URI - [%s] %s, Destination - %s",
            annotationName,
            request.getMethod(),
            request.getURI(),
            request.getHost() + ":" + request.getPort());
  }

  @Override
  public void onQueued(Request request) {
    // Custom logic to execute when a request begins
    LOG.info("[%s] Request queued for URI - [%s] %s, Destination - %s",
            annotationName,
            request.getMethod(),
            request.getURI(),
            request.getHost() + ":" + request.getPort());
  }
}
